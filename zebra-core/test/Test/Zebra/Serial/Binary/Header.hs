{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Header where

import           Data.Map (Map)
import qualified Data.Map as Map

import           Hedgehog
import qualified Hedgehog.Gen as Gen

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Zebra.Serial.Binary.Data
import           Zebra.Serial.Binary.Header


prop_roundtrip_header_v2 :: Property
prop_roundtrip_header_v2 =
  gamble (mapOf jAttributeName $ fmap columnSchemaV0 jColumnSchema) $
    trippingSerial bHeaderV2 getHeaderV2

prop_roundtrip_header_v3 :: Property
prop_roundtrip_header_v3 =
  gamble jTableSchema $
    trippingSerial bHeaderV3 getHeaderV3

prop_roundtrip_header :: Property
prop_roundtrip_header =
  gamble jHeader $
    trippingSerial bHeader getHeader

jHeader :: Gen Header
jHeader =
  Gen.choice [
      HeaderV3 <$> jTableSchema
    , HeaderV2 <$> mapOf jAttributeName (fmap columnSchemaV0 jColumnSchema)
    ]

mapOf :: Ord k => Gen k -> Gen v -> Gen (Map k v)
mapOf k v =
  Map.fromList <$> listOf ((,) <$> k <*> v)

tests :: IO Bool
tests =
  checkParallel $$(discover)
