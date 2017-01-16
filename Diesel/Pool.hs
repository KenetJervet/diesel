{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-|
Module      : Diesel.Pool
Description : A high performance resource pool
Copyright   : (c) Winterland, 2017
License     : BSD
Maintainer  : drkoster@qq.com
Stability   : experimental
Portability : PORTABLE

This module provide a high performance resource pool, the difference from
<resource-pool http://hackage.haskell.org/package/resource-pool> package is as following:

  * Use 'MVar' instead of 'TVar' to achieve blocking, since 'MVar' has a nice one-thread wake
    property.

  * Instead of using system time as time source, we use 'AtomicCounter' from <atomic-primops
    http://hackage.haskell.org/package/atomic-primops> to track time, this will not affect
    precision because the reaper thread is still running at a fixed rate(once per second).

  * Provide built-in timeout for thread holding a resource, this will make a lot of
    operation easier and guarantee the pool will not be locked by its workers.

  * Provide 'closePool' to clean resources instead of relying on 'Weak' finalizers.

-}

module Diesel.Pool
    ( TimeoutException
    , Pool
    , createPool
    , purgePool
    , closePool
    , withResource
    ) where

import           Control.Concurrent
import           Control.Concurrent.MVar
import qualified Control.Exception       as E
import           Control.Monad
import           Data.Atomics.Counter
import           Data.IORef
import           Data.Typeable
import qualified Data.Vector             as V

data TimeoutException = TimeoutException deriving (Typeable, Show)

instance E.Exception TimeoutException

data Worker = Worker
    { workerRef :: {-# UNPACK #-} !(IORef (Maybe ThreadId))
    , startTime :: {-# UNPACK #-} !Int
    }

data Entry a = Entry
    { entry        :: a
    , lastUsedTime :: {-# UNPACK #-} !Int
    }

data Pool a = Pool
    { stripNum           :: {-# UNPACK #-} !Int
    , maxResPerLocalPool :: {-# UNPACK #-} !Int
    , resourceTimeout    :: {-# UNPACK #-} !Int
    , workerTimeout      :: {-# UNPACK #-} !Int
    , localPools         :: {-# UNPACK #-} !(V.Vector (LocalPool a))
    , reaperThreadId     :: {-# UNPACK #-} !(IORef (Maybe ThreadId))
    , poolTimer          :: {-# UNPACK #-} !AtomicCounter
    , create             :: IO a
    , destroy            :: a -> IO ()
    }

data LocalPool a = LocalPool
    { resCounter   :: {-# UNPACK #-} !AtomicCounter
    , resourceList :: {-# UNPACK #-} !(IORef [Entry a])
    , resourceLock :: {-# UNPACK #-} !(MVar (Entry a))
    , workerList   :: {-# UNPACK #-} !(IORef [Worker])
    }

-- | Try to acquire a resource from pool and do some work within given time, then put it back.
--
-- Thread will be blocked until a resource is acquired, once hold the resource,
-- the work has to be finished within given time, otherwise a 'TimeoutException' will be thrown.
--
-- If any exception are thrown during working(including 'TimeoutException' above), the
-- resource will be destroyed, since it's impossible to know the resource's state.
-- These exceptions will be re-rised and you can use combinators from "Control.Exception"
-- such as 'E.tryJust' to handle them.
--
withResource :: Pool a        -- ^ The resource pool
             -> Int           -- ^ Timeout
             -> (a -> IO b)   -- ^ the work to do
             -> IO b
withResource Pool{..} timeout act = E.mask $ \ restore -> do
    tid <- myThreadId
    (cap, _) <- threadCapability tid
    let LocalPool{..} = localPools V.! (cap `mod` stripNum)
    -- first try to get resource from resource list,
    -- try to take the locked resource only if the list is empty.
    r <- E.bracket
        (atomicModifyIORef' resourceList $ \ es ->
            case es of e:es' -> (es', Just e)
                       []    -> ([], Nothing))
        (\ me -> case me of
            Just e -> do
                unlock <- tryPutMVar resourceLock e
                unless unlock $ atomicModifyIORef resourceList (\ es -> (e:es, ()))
            Nothing -> return ())
        (\ me -> do
            case me of
                Just Entry{..} -> return entry
                Nothing -> E.bracketOnError (incrCounter 1 resCounter)
                    (\ count -> when (count > maxResPerLocalPool) (incrCounter_ (-1) resCounter))
                    (\ count ->
                        if count > maxResPerLocalPool
                        then entry <$> takeMVar resourceLock
                        else create))

    -- put worker into worker list
    workerRef <- newIORef (Just tid)
    startTime <- readCounter poolTimer
    let w = Worker{workerRef, startTime}
    atomicModifyIORef workerList $ \ ws -> (w:ws, ())

    -- E.onException will re-throw any exception it capture
    ret <- restore (act r) `E.onException` (do
        -- let's remove worker from worker list first
        -- so that we won't receive new 'TimeoutException'.
        atomicWriteIORef workerRef Nothing
        E.catch (destroy r) $ \ (_ :: E.SomeException) -> return ()
        incrCounter_ (-1) resCounter)

    -- put resource back
    now <- readCounter poolTimer
    let e = Entry r now
    unlock <- tryPutMVar resourceLock e
    unless unlock $ atomicModifyIORef resourceList (\ es -> (e:es, ()))
    return ret

createPool :: IO a  -- ^ Action that creates a new resource.
           -> (a -> IO ()) -- ^ Action that destroys an existing resource.
           -> Int   -- ^ The number of stripes (distinct sub-pools) to maintain.
                    -- The smallest acceptable value is 1.
           -> Int   -- ^ Maximum number of resources to keep open per stripe.
                    -- The smallest acceptable value is 1.
           -> Int   -- Amount of seconds for which an unused resource is kept open.
                    -- The smallest acceptable value is 1.
           -> Int   -- ^ Maximum seconds a thread can hold a resource.
                    -- The smallest acceptable value is 1.
           -> IO (Pool a)
createPool create destroy stripNum maxResPerLocalPool resourceTimeout workerTimeout = do
    when (stripNum < 1) $
        error $ "Diesel.Pool.createPool: invalid stripe count " ++ show stripNum
    when (maxResPerLocalPool < 1) $
        error $ "Diesel.Pool.createPool: invalid maximum resource count " ++ show maxResPerLocalPool
    when (resourceTimeout < 1) $
        error $ "Diesel.Pool.createPool: invalid resource timeout " ++ show resourceTimeout
    when (workerTimeout < 1) $
        error $ "Diesel.Pool.createPool: invalid worker timeout " ++ show workerTimeout
    localPools <- V.replicateM stripNum $
        LocalPool <$> newCounter 0 <*> newIORef [] <*> newEmptyMVar <*> newIORef []
    poolTimer <- newCounter 0
    reaperThreadId <- newIORef . Just =<< forkIO
        (reaper resourceTimeout workerTimeout poolTimer localPools destroy)
    return $ Pool
        { stripNum
        , maxResPerLocalPool
        , resourceTimeout
        , workerTimeout
        , localPools
        , reaperThreadId
        , poolTimer
        , create
        , destroy
        }
  where
    reaper resourceTimeout workerTimeout poolTimer localPools destroy = forever $ do
        threadDelay 1000000
        now <- incrCounter 1 poolTimer
        V.forM_ localPools $ \ LocalPool{..} -> do
            -- swap an empty list
            ws <- atomicModifyIORef workerList (\ ws -> ([], ws))
            -- scan worker list and throw exception to timeout workers
            ws' <- foldM (\ acc w@Worker{..} -> do
                wid <- readIORef workerRef
                case wid of
                    Just wid' ->
                        if startTime + resourceTimeout < now
                        then throwTo wid' TimeoutException >> return acc
                        else return (w:acc)
                    Nothing -> return acc
                ) [] ws
            -- merge current worker list
            -- the list thunk will be traversed in next scan
            atomicModifyIORef workerList (\ ws -> (ws ++ ws', ()))

            -- take the lock first
            r <- tryTakeMVar resourceLock
            case r of
                -- let's scan idle resource
                Just r -> do
                    -- swap an empty list
                    rs <- atomicModifyIORef' resourceList $ \ rs -> ([], rs)
                    forM_ (r:rs) $ \ e@Entry{..} ->
                        if lastUsedTime + resourceTimeout < now
                        then do
                            E.catch (destroy entry) $ \ (_ :: E.SomeException) -> return ()
                            incrCounter_ (-1) resCounter
                        else do
                            unlock <- tryPutMVar resourceLock e
                            unless unlock $ atomicModifyIORef resourceList (\ es -> (e:es, ()))
                -- all resource are in used, let's scan idle resource later
                Nothing -> return ()


-- | Destroy all idle resource
--
purgePool:: Pool a -> IO ()
purgePool Pool{..} = V.forM_ localPools purgeLocalPool
  where
    purgeLocalPool LocalPool{..} = do
        -- take the lock first
        r <- tryTakeMVar resourceLock
        rs <- atomicModifyIORef' resourceList $ \ rs -> ([], rs)
        let rs' = maybe rs (:rs) r
        forM_ rs' $ \ Entry{..} -> do
            E.catch (destroy entry) $ \ (_ :: E.SomeException) -> return ()
            incrCounter_ (-1) resCounter

-- | Clean up all resources.
--
-- This function will block until all resource are returned and destroyed, so please make
-- sure no thread is try to acquire resources, and you should expect all resources
-- would be cleaned up within worker timeout, eg. the maximum time a resource can be held.
--
closePool :: Pool a -> IO ()
closePool p@Pool{..} = do
    V.forM_ localPools closeLocalPool
    rid <- readIORef reaperThreadId
    writeIORef reaperThreadId Nothing
    case rid of Just rid' -> killThread rid'
                _ -> return ()
  where
    closeLocalPool lp@LocalPool{..} = do
        remaining <- readCounter resCounter
        when (remaining > 0) $ do
            r <- tryTakeMVar resourceLock
            rs <- atomicModifyIORef' resourceList $ \ rs -> ([], rs)
            let rs' = maybe rs (:rs) r
            forM_ rs' $ \ Entry{..} -> do
                E.catch (destroy entry) $ \ (_ :: E.SomeException) -> return ()
                incrCounter_ (-1) resCounter
            -- continue until we close all resource
            closeLocalPool lp
