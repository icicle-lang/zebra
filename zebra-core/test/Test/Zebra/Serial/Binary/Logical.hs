{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Logical where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Viking.ByteStream as ByteStream
import qualified Viking.Stream as Stream

import           Zebra.Serial.Binary.Logical


data BinaryError =
    BinaryEncode !BinaryLogicalEncodeError
  | BinaryDecode !BinaryLogicalDecodeError
    deriving (Eq, Show)

prop_roundtrip_file :: Property
prop_roundtrip_file = property $ do
  schema  <- forAll jTableSchema
  logical <- forAll (Gen.list (Range.linear 1 10) $ jSizedLogical1 schema)
  trippingBoth
    logical
    (first BinaryEncode . withList (ByteStream.toChunks . encodeLogical schema))
    (first BinaryDecode . withList (Stream.effect . fmap snd . decodeLogical . ByteStream.fromChunks))


tests :: IO Bool
tests =
  checkParallel $$(discover)
