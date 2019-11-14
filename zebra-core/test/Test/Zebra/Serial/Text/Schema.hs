{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Text.Schema where

import           Hedgehog

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Text.Schema


prop_roundtrip_schema :: Property
prop_roundtrip_schema =
  gamble jTableSchema $ \x ->
    trippingBoth x (pure . encodeSchemaWith TextV0) (decodeSchema)

tests :: IO Bool
tests =
  checkParallel $$(discover)
