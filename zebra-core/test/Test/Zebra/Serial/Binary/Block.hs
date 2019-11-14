{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Block where

import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Vector as Boxed
import qualified Data.Vector.Unboxed as Unboxed

import           Hedgehog

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Text.Printf (printf)
import           Text.Show.Pretty (ppShow)

import           Zebra.Factset.Block
import           Zebra.Factset.Data
import           Zebra.Serial.Binary.Block
import           Zebra.Serial.Binary.Data
import           Zebra.Table.Data
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped


prop_roundtrip_from_facts :: Property
prop_roundtrip_from_facts = property $ do
  version <- forAll jBinaryVersion
  schema <- forAll jColumnSchema
  facts <- forAll (listOf $ jFact schema (AttributeId 0))
  let
    schemas =
      Boxed.singleton schema

    header =
      headerOfAttributes version $ Map.singleton (AttributeName "attribute_0") schema

    block =
      either (Savage.error . show) id .
      blockOfFacts schemas $
      Boxed.fromList facts

  annotate (ppShow schema)
  trippingSerialE (bBlock header) (getBlock header) block

prop_roundtrip_block :: Property
prop_roundtrip_block =
  gamble jYoloBlock $ \block ->
    let
      mkAttr (ix :: Int) attr0 =
        (AttributeName . Text.pack $ printf "attribute_%05d" ix, attr0)

      header =
        headerOfAttributes BinaryV2 .
        Map.fromList $
        List.zipWith mkAttr [0..] .
        fmap (unsafeTakeArray . Striped.schema) .
        Boxed.toList $
        blockTables block
    in
      trippingSerialE (bBlock header) (getBlock header) block

prop_roundtrip_entities :: Property
prop_roundtrip_entities =
  gamble (Boxed.fromList <$> listOf jBlockEntity) $
    trippingSerial bEntities getEntities

prop_roundtrip_attributes :: Property
prop_roundtrip_attributes =
  gamble (Unboxed.fromList <$> listOf jBlockAttribute) $
    trippingSerial bAttributes getAttributes

prop_roundtrip_indices :: Property
prop_roundtrip_indices =
  gamble (Unboxed.fromList <$> listOf jBlockIndex) $
    trippingSerial bIndices getIndices

prop_roundtrip_tables :: Property
prop_roundtrip_tables =
  gamble (Boxed.fromList <$> listOf (jStripedColumn 1)) $ \xs ->
    trippingSerialE bTables (getTables $ fmap Striped.schemaColumn xs) (fmap (Striped.Array DenyDefault) xs)

unsafeTakeArray :: Schema.Table -> Schema.Column
unsafeTakeArray =
  either (Savage.error . ppShow) snd . Schema.takeArray

tests :: IO Bool
tests =
  checkParallel $$(discover)
