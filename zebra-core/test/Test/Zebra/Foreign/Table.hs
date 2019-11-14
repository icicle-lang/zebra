{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Table where

import           Anemone.Foreign.Mempool (Mempool)
import qualified Anemone.Foreign.Mempool as Mempool

import           Control.Monad.Trans.Resource (allocate, runResourceT)
import           Control.Monad.Morph (hoist)

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Control.Monad.Trans.Either (EitherT)

import           Zebra.Foreign.Table
import           Zebra.Table.Striped (Table)
import qualified Zebra.Table.Striped as Striped

prop_roundtrip_table :: Property
prop_roundtrip_table =
  gamble jSizedStriped $ \table ->
    hoist runResourceT $ do
      (_, pool) <- allocate Mempool.create Mempool.free
      trippingIO (liftE . foreignOfTable pool) tableOfForeign table

prop_deep_clone_table :: Property
prop_deep_clone_table =
  property $
    testClone id deepCloneTable

prop_neritic_clone_table :: Property
prop_neritic_clone_table =
  property $
    testClone id neriticCloneTable

prop_agile_clone_table :: Property
prop_agile_clone_table =
  property $
    testClone Striped.schema agileCloneTable

prop_grow_table :: Property
prop_grow_table =
  gamble (Gen.int (Range.linear 0 100)) $ \n ->
    testClone Striped.schema (\pool table -> growTable pool table n >> pure table)

testClone :: (Show a, Show x, Eq a, Eq x) => (Table -> a) -> (Mempool -> CTable -> EitherT x IO CTable) -> PropertyT IO ()
testClone select clone = do
  table <- forAll jSizedStriped
  test . hoist runResourceT $ do
    (_, pool) <- allocate  Mempool.create Mempool.free
    trippingByIO select (bind (clone pool) . foreignOfTable pool) tableOfForeign table


tests :: IO Bool
tests =
  checkParallel $$(discover)
