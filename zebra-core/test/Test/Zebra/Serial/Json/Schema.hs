{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Json.Schema where

import           Hedgehog

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Json.Schema


prop_roundtrip_schema_v0 :: Property
prop_roundtrip_schema_v0 =
  gamble (tableSchemaV0 <$> jTableSchema) $
    trippingBoth `flip` (pure . encodeSchema SchemaV0) `flip` (decodeSchema SchemaV0)

prop_roundtrip_schema_v1 :: Property
prop_roundtrip_schema_v1 =
  gamble jTableSchema $
    trippingBoth `flip` (pure . encodeSchema SchemaV1) `flip` (decodeSchema SchemaV1)

tests :: IO Bool
tests =
  checkParallel $$(discover)
