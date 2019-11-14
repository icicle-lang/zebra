{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Text.Striped where


import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Viking.ByteStream as ByteStream

import           Zebra.Serial.Text.Striped
import qualified Zebra.Table.Striped as Striped


data TextError =
    TextEncode !TextStripedEncodeError
  | TextDecode !TextStripedDecodeError
    deriving (Eq, Show)

prop_roundtrip_table :: Property
prop_roundtrip_table = property $ do
  schema   <- forAll jTableSchema
  logical  <- forAll (jSizedLogical schema)
  let
    Right striped =
      Striped.fromLogical schema logical

  trippingBoth
    striped
    (first TextEncode . encodeStripedBlock)
    (first TextDecode . decodeStripedBlock schema)

prop_roundtrip_file :: Property
prop_roundtrip_file = property $ do
  schema   <- forAll jTableSchema
  logical  <- forAll (Gen.list (Range.linear 1 10) $ jSizedLogical1 schema)

  let
    takeStriped x =
      let
        Right striped =
          Striped.fromLogical schema x
      in
        striped

  trippingBoth
    (fmap takeStriped logical)
    (first TextEncode . withList (ByteStream.toChunks . encodeStriped))
    (first TextDecode . withList (decodeStriped schema . ByteStream.fromChunks))


tests :: IO Bool
tests =
  checkParallel $$(discover)
