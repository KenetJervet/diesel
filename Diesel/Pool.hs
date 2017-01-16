{-# LANGUAGE BangPattern #-}

module Diesel.Pool where

import Data.Vector
import Data.IORef
import Data.Atomics.Counter
import qualified Control.Exception as E

data TimeoutException = TimeoutException

data Worker = Worker
    { workerRef    :: {-# UNPACK #-} !(IORef (Maybe ThreadId))
    , startTime    :: {-# UNPACK #-} !Int
    }

data Entry a = Entry
    { entry :: a
    , lastUsedTime :: {-# UNPACK #-} !Int
    }

data Pool a = Pool
    {   stripNum           :: Int
    ,   poolTimer          :: AtomicCounter
    ,   maxResPerLocalPool :: Int
    ,   resourceTimeout    :: Int
    ,   workerTimeout      :: Int
    ,   localPools         :: V.Vector LocalPool
    ,   create             :: IO a
    ,   destroy            :: a -> IO ()
    ,   reaperThreadId     :: {-# UNPACK #-} !(IORef (Maybe ThreadId))
    }

data LocalPool a = LocalPool
    {   resCounter     :: AtomicCounter
    ,   resourceList   :: IORef [a]
    ,   resourceLock   :: MVar a
    ,   workerList     :: IORef [Worker]
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
withResource pool@Pool{..} timeout act = mask $ \ restore -> do
    (a, local) <- takeResource pool
    -- E.onException will re-throw any exception it capture
    ret <- restore (act resource) `E.onException` destroyResource pool local resource
    now <- readCounter poolTimer
    putResource local (Entry resource now)
    return ret

createPool :: Int     -- ^ The number of stripes (distinct sub-pools) to maintain.
                      -- The smallest acceptable value is 1.
           -> Int     -- ^ Maximum number of resource to keep open per stripe.  The
                      -- smallest acceptable value is 1.
           -> Int     -- ^ Amount of seconds between each run of reaper
           -> IO a
           -> IO Pool
createPool stripNum maxResPerLocalPool = do

purgePool:: Pool -> IO ()
purgePool Pool{..} = V.forM_ localPools purgeLocalPool
  where
    purgeLocalPool LocalPool{..} = do
        -- take the lock first
        r <- tryTakeMVar resourceLock
        rs <- atomicModifyIORef' resourceList $ \ rs -> ([], rs)
        let rs' = maybe rs (:rs) r
            !len = length rs'
        writeCounter resCounter len
        forM_ rs' $ \ r ->
            destroy resource `E.catch` \ (_ :: E.SomeException) -> return ()


closePool :: Pool -> IO ()
closePool p@Pool{..} = do
    purgePool p


--------------------------------------------------------------------------------
-- Internal
--------------------------------------------------------------------------------

reaper :: Int -> Pool{..} -> IO ()
reaper delay pool = forever $
    threadDelay (delay * 1000000)
    now <- incrCounter 1 poolTimer
    V.forM_ localPools $ \ lp@LocalPool{..} -> do
        -- swap an empty list
        ws <- atomicWriteIORef workerList $ \ ws -> ([], ws)
        -- scan worker list and throw exception to timeout workers
        ws' <- foldM (\ acc Worker{..} ->
            w <- workerRef
            case w of
                Just tid ->
                    if startTime + expireTime < now
                    then throwTo tid TimeoutException >> return acc
                    else return (w:acc)
                Nothing -> return acc
        ) [] ws
        -- merge current worker list
        -- the list thunk will be traversed in next scan
        atomicWriteIORef workerList $ \ ws -> (ws ++ ws', ())

        -- take the lock first
        r <- tryTakeMVar resourceLock
        case r of
            -- let's scan idle resource
            Just r ->
                -- swap an empty list
                rs <- atomicModifyIORef' resourceList $ \ rs -> ([], rs)
                forM_ (r:rs) $ \ r@Entry{..} ->
                    if lastUsedTime + resourceTimeout < now
                    then destroyResource pool lp entry
                    else putResource lp r
            -- all resource are in used, let's scan idle resource later
            Nothing -> return ()


-- first try to unlock waiting thread with resource,
-- put back to resource list only if put fail.
--
putResource :: LocalPool a -> Entry a -> IO ()
putResource LocalPool{..} r = do
    unlock <- tryPutMVar resourceLock r
    unless unlock $ atomicModifyIORef resourceList (\ rs -> (r:rs, ()))
{-# INLINABLE putResource #-}

-- Destroy a resource. Note that this will ignore any exceptions in the
-- destroy function.
--
destroyResource :: Pool a -> LocalPool a -> a -> IO ()
destroyResource Pool{..} LocalPool{..} resource = do
    destroy resource `E.catch` \(_::SomeException) -> return ()
    incrCounter (-1) resCounter
{-# INLINABLE destroyResource #-}

takeResource :: Pool a -> (a, LocalPool a)
takeResource Pool{..} act = do
    tid <- myThreadId
    tidRef <- newIORef (Just tid)
    let lp@LocalPool{..} = localPools  V.! (tid `mod` stripNum)
    -- first try to get resource from resource list,
    -- try to take the locked resource only if the list is empty.
    r <- atomicModifyIORef' resourceList $ \ rs ->
        case rs of r:rs' -> (rs', Just r)
                   []    -> ([], Nothing)
    case r of
        Just r' -> return r'
        Nothing -> bracketOnError (incrCounter 1 resCounter)
            (\ count -> when (count > maxResNum) (incrCounter (-1) resCounter))
            (\ count ->
                if count > maxResNum
                then takeMVar resourceLock
                else create)
{-# INLINABLE takeResource #-}
