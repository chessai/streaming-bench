{-# language
    BangPatterns
  , ImportQualifiedPost
  , PackageImports
  , RecordWildCards
  , ScopedTypeVariables
  , ViewPatterns
#-}

module Streaming.Bench
  ( bench
  ) where

import "base" System.Mem (performGC)
import "base" Data.Monoid (Ap(..))
import "base" Control.Exception qualified as E
import "base" Data.Word (Word64)
import "streaming" Streaming.Prelude (Stream, Of(..))
import "streaming" Streaming.Prelude qualified as S
import "base" GHC.Clock (getMonotonicTimeNSec)
import "base" Data.Fixed (Fixed(..), HasResolution, E3, div')
import "base" Control.Concurrent.MVar
import "base" Text.Printf (printf)

-- All times are currently in wall time.
data Result = Result
  { totalTime :: {-# unpack #-} !Word64
  , totalElements :: {-# unpack #-} !Word64
  , rangeTime :: {-# unpack #-} !Word64
  , meanTime :: {-# unpack #-} !Double
  , stdevTime :: {-# unpack #-} !Double
  }

prettyResult :: Result -> String
prettyResult Result{..} = unlines
  [ "Time elapsed: " ++ showWord64Nanos3 totalTime
  , "Range: " ++ showWord64Nanos3 rangeTime
  , "Stdev: " ++ showDoubleNanos3 stdevTime
  , "Mean: " ++ showDoubleNanos3 meanTime
  ]

data IntermediateResult = IntermediateResult
  { itotalTime :: {-# unpack #-} !Word64
  , itotalElements :: {-# unpack #-} !Word64
  , imaxTime :: {-# unpack #-} !Word64
  , iminTime :: {-# unpack #-} !Word64
  }

instance Semigroup IntermediateResult where
  r1 <> r2 = IntermediateResult
    { itotalTime = itotalTime r1 + itotalTime r2
    , itotalElements = itotalElements r1 + itotalElements r2
    , imaxTime = max (imaxTime r1) (imaxTime r2)
    , iminTime = min (iminTime r1) (iminTime r2)
    }

instance Monoid IntermediateResult where
  mempty = IntermediateResult
    { itotalTime = 0
    , itotalElements = 0
    , imaxTime = minBound
    , iminTime = maxBound
    }

bench :: forall a r. Stream (Of a) IO r -> IO ()
bench s0 = do
  performGC
  let i0 = mempty
  let o0 = OnlineVariance
        { n = 0
        , mean = 0.0
        , m2 = 0.0
        }
  (IntermediateResult{..}, OnlineVariance{..}) <- go s0 i0 o0
  putStrLn $ prettyResult $ Result
    { totalTime = itotalTime
    , totalElements = itotalElements
    , rangeTime = imaxTime - iminTime
    , stdevTime = sqrt (m2 / (word64ToDouble n - 1))
    , meanTime = mean
    }
  where
    go :: Stream (Of a) IO r -> IntermediateResult -> OnlineVariance -> IO (IntermediateResult, OnlineVariance)
    go stream !accI accO@OnlineVariance{..} = do
      (timeElapsed, e) <- stopwatch_ (S.next stream)
      case e of
        Left r -> do
          pure (accI, accO)
        Right (_, rest) -> do

          let iresult = IntermediateResult
                { itotalTime = timeElapsed
                , itotalElements = 1
                , imaxTime = timeElapsed
                , iminTime = timeElapsed
                }
          let i = iresult <> accI

          let x = word64ToDouble timeElapsed
          let n1 = n + 1
          let delta = x - mean
          let o = OnlineVariance
                { n = n1
                , mean = mean + delta / word64ToDouble n1
                , m2 = m2 + delta * (x - mean)
                }

          go rest (iresult <> accI) o

stopwatch_ :: IO a -> IO (Word64, a)
stopwatch_ io = do
  start <- getMonotonicTimeNSec
  a <- E.evaluate =<< io
  end <- getMonotonicTimeNSec
  pure (end - start, a)

data OnlineVariance = OnlineVariance
  { n :: {-# unpack #-} !Word64
  , mean :: {-# unpack #-} !Double
  , m2 :: {-# unpack #-} !Double
  }

onlineVariance :: forall m r. Monad m => Stream (Of Double) m r -> m Double
onlineVariance s0 = do
  let o0 = OnlineVariance
        { n = 0
        , mean = 0.0
        , m2 = 0.0
        }
  OnlineVariance{..} <- go s0 o0
  pure (m2 / (word64ToDouble n - 1))
  where
    go :: Stream (Of Double) m r -> OnlineVariance -> m OnlineVariance
    go stream acc@OnlineVariance{..} = do
      e <- S.next stream
      case e of
        Left r -> do
          pure acc
        Right (x, rest) -> do
          let !n1 = n + 1
          let !delta = x - mean
          let o = OnlineVariance
                { n = n1
                , mean = mean + delta / word64ToDouble n1
                , m2 = m2 + delta * (x - mean)
                }
          go rest o

word64ToDouble :: Word64 -> Double
word64ToDouble = fromIntegral

showWord64Nanos3 :: Word64 -> String
showWord64Nanos3 w = showDoubleNanos3 (word64ToDouble w)

showDoubleNanos3 :: Double -> String
showDoubleNanos3 (times 1000 -> d)
  | d < 995   = printf "%3.0f ps" d
  | d < 995e1 = printf "%3.1f ns" (d / 1e3)
  | d < 995e3 = printf "%3.0f ns" (d / 1e3)
  | d < 995e4 = printf "%3.1f μs" (d / 1e6)
  | d < 995e6 = printf "%3.0f μs" (d / 1e6)
  | d < 995e7 = printf "%3.1f ms" (d / 1e9)
  | d < 995e9 = printf "%3.0f ms" (d / 1e9)
  | otherwise = printf "%4.2f s"  (d / 1e12)

times :: Num a => a -> a -> a
times x y = x * y
