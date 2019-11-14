{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Json.Striped where

import           Hedgehog

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Json.Striped
import qualified Zebra.Table.Striped as Striped


data JsonError =
    JsonEncode !JsonStripedEncodeError
  | JsonDecode !JsonStripedDecodeError
    deriving (Eq, Show)

prop_roundtrip_table :: Property
prop_roundtrip_table = property $ do
  schema  <- forAll jTableSchema
  logical <- forAll (jSizedLogical schema)
  let
    Right striped =
      Striped.fromLogical schema logical

  trippingBoth
    striped
    (first JsonEncode . encodeStriped)
    (first JsonDecode . decodeStriped schema)

tests :: IO Bool
tests =
  checkParallel $$(discover)
