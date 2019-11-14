{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Text.Logical where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Viking.ByteStream as ByteStream

import           Zebra.Serial.Text.Logical


data TextError =
    TextEncode !TextLogicalEncodeError
  | TextDecode !TextLogicalDecodeError
    deriving (Eq, Show)

prop_roundtrip_table :: Property
prop_roundtrip_table = property $ do
  schema   <- forAll jTableSchema
  logical  <- forAll (jSizedLogical schema)
  trippingBoth
    logical
    (first TextEncode . encodeLogicalBlock schema)
    (first TextDecode . decodeLogicalBlock schema)

prop_roundtrip_file :: Property
prop_roundtrip_file = property $ do
  schema   <- forAll jTableSchema
  logical  <- forAll (Gen.list (Range.linear 1 10) $ jSizedLogical1 schema)

  trippingBoth
    logical
    (first TextEncode . withList (ByteStream.toChunks . encodeLogical schema))
    (first TextDecode . withList (decodeLogical schema . ByteStream.fromChunks))

tests :: IO Bool
tests =
  checkParallel $$(discover)
