module Diesel.Pool where

import Data.Vector
import Data.IORef
import Data.Atomics.Counter


data TimeoutException = TimeoutException

data Worker = Worker
    { workerRef    :: {-# UNPACK #-} !(IORef (Maybe ThreadId))
    , startTick    :: {-# UNPACK #-} !Int
    }

data Entry a = Entry
    { entry :: a
    , lastUsedTick :: {-# UNPACK #-} !Int
    }

data Pool a = {
        stripNum   :: Int
    ,   expireTime :: Int
    ,   localPools :: V.Vector LocalPool
    ,   create     :: IO a
    ,   destroy    :: a -> IO ()
    }

data LocalPool a = {
        maxResNum      :: Int
    ,   poolTimer      :: AtomicCounter
    ,   resCounter     :: AtomicCounter
    ,   resourceList   :: IORef [a]
    ,   resourceLock   :: MVar a
    ,   workerList     :: IORef [Worker]
    ,   reaperThreadId :: ThreadId
    }


withResource :: Pool a -> Int -> (a -> IO b) -> IO b
withResource pool timeout act = mask $ \ restore -> do
    (resource, local) <- takeResource pool
    ret <- restore (act resource) `onException`
        destroyResource pool local resource
    putResource local resource
    return ret
  where
    takeResource :: Pool a -> (Worker, a, LocalPool a)
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

    -- first try to unlock waiting thread with resource,
    -- put back to resource list only if put fail.
    --
    putResource :: LocalPool a -> a -> IO ()
    putResource LocalPool{..} r = do
        unlock <- tryPutMVar resourceLock r
        unless unlock $ atomicModifyIORef resourceList (\ rs -> (r:rs, ()))

    destroyResource :: Pool a -> LocalPool a -> a -> IO ()
    destroyResource Pool{..} LocalPool{..} resource = do
        destroy resource `E.catch` \(_::SomeException) -> return ()
        incrCounter (-1) resCounter

createPool :: Int     -- ^ The number of stripes (distinct sub-pools) to maintain.
                      -- The smallest acceptable value is 1.
           -> Int     -- ^ Maximum number of 'Connection' to keep open per stripe.  The
                      -- smallest acceptable value is 1.
           -> Int     -- ^ Amount of seconds between each run of reaper
           -> IO a
           -> IO Pool
createPool =

reaper :: Int -> V.Vector (LocalPool a) -> IO ()
reaper delay pools = forever $
    threadDelay (delay * 1000000)
    V.forM_ pools $ \ LocalPool{..} -> do
        ws <- readIORef workerList
        ws' <- foldM (\ acc Worker{..} ->
            w <- workerRef
            case w of
                Just tid ->
                    st <- incrCounter scanCounter
                    when (st > expireTime) $
                        throwTo tid TimeoutException
                    return (w:acc)
                Nothing -> return acc
        ) [] ws
        atomicWriteIORef workerList ws'

        atomicModifyIORef' resourceList $ \ rs ->





destroyPool :: Pool -> IO ()


