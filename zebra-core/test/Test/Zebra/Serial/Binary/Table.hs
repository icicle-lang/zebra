{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Table where

import           Hedgehog

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Serial.Binary.Table
import qualified Zebra.Table.Striped as Striped


prop_roundtrip_table :: Property
prop_roundtrip_table = property $ do
  version <- forAll jBinaryVersion
  table   <- forAll (jStriped 1)
  trippingSerialE (bTable version) (getTable version 1 $ Striped.schema table) table

prop_roundtrip_column :: Property
prop_roundtrip_column = property $ do
  version <- forAll jBinaryVersion
  column  <- forAll (jStripedColumn 1)

  trippingSerialE (bColumn version) (getColumn version 1 $ Striped.schemaColumn column) column


tests :: IO Bool
tests =
  checkParallel $$(discover)
