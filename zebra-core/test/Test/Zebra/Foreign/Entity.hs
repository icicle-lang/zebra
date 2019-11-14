{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Entity where

import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.Trans.Resource (allocate, runResourceT)
import           Control.Monad.Morph (hoist)

import           Hedgehog

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Foreign.Entity


prop_roundtrip_entity :: Property
prop_roundtrip_entity =
  gamble jEntity $ \entity ->
    hoist runResourceT $ do
      (_, pool) <- allocate Mempool.create Mempool.free
      trippingIO (liftE . foreignOfEntity pool) entityOfForeign entity


tests :: IO Bool
tests =
  checkParallel $$(discover)
