{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Striped where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Viking.ByteStream as ByteStream

import           Zebra.Serial.Binary.Striped
import qualified Zebra.Table.Striped as Striped


data BinaryError =
    BinaryEncode !BinaryStripedEncodeError
  | BinaryDecode !BinaryStripedDecodeError
    deriving (Eq, Show)

prop_roundtrip_file :: Property
prop_roundtrip_file = withTests 1000 . property $ do
  schema <- forAll jTableSchema
  logical <- forAll (Gen.list (Range.linear 1 10) $ jSizedLogical1 schema)
  let
    takeStriped x =
      let
        Right striped =
          Striped.fromLogical schema x
      in
        striped

  trippingBoth
    (fmap takeStriped logical)
    (first BinaryEncode . withList (ByteStream.toChunks . encodeStriped))
    (first BinaryDecode . withList (decodeStriped . ByteStream.fromChunks))


tests :: IO Bool
tests =
  checkParallel $$(discover)
