{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Json.Logical where

import           Hedgehog

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Json.Logical


data JsonError =
    JsonEncode !JsonLogicalEncodeError
  | JsonDecode !JsonLogicalDecodeError
    deriving (Eq, Show)

prop_roundtrip_table :: Property
prop_roundtrip_table = property $ do
  schema <- forAll jTableSchema
  table  <- forAll (jSizedLogical schema)
  trippingBoth
    table
    (first JsonEncode . encodeLogical schema)
    (first JsonDecode . decodeLogical schema)

prop_roundtrip_value :: Property
prop_roundtrip_value = property $ do
  schema <- forAll jColumnSchema
  value  <- forAll (jLogicalValue schema)
  trippingBoth
    value
    (first JsonEncode . encodeLogicalValue schema)
    (first JsonDecode . decodeLogicalValue schema)

tests :: IO Bool
tests =
  checkParallel $$(discover)
