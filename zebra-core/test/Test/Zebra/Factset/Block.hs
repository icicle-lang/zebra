{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Factset.Block where

import           Control.Monad.Trans.Either (hoistEither)

import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Vector as Boxed

import           Hedgehog

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           Zebra.Factset.Block
import           Zebra.Factset.Data
import           Zebra.Factset.Table
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Striped as Striped


prop_roundtrip_facts :: Property
prop_roundtrip_facts = property $ do
  schema <- forAll jColumnSchema
  facts  <- forAll (listOf $ jFact schema (AttributeId 0))
  let
    schemas =
      Boxed.singleton schema

    input =
      Boxed.fromList facts

  trippingBoth input (blockOfFacts schemas) factsOfBlock

prop_roundtrip_tables :: Property
prop_roundtrip_tables =
  gamble jBlock $ \block ->
  let
    names =
      fmap (AttributeName . Text.pack . ("attribute_" <>) . show)
        [0..Boxed.length (blockTables block) - 1]
  in
    trippingBoth block (tableOfBlock $ Boxed.fromList names) blockOfTable

prop_roundtrip_attribute_schemas :: Property
prop_roundtrip_attribute_schemas =
  gamble (listOf jColumnSchema) $ \attrs0 ->
  let
    mkAttr (ix :: Int) attr0 =
      (AttributeName . Text.pack $ "attribute_" <> show ix, attr0)

    attrs =
      Map.fromList $
      List.zipWith mkAttr [0..] attrs0
  in
    trippingBoth attrs (pure . tableSchemaOfAttributes) attributesOfTableSchema

prop_logical_from_block_is_valid :: Property
prop_logical_from_block_is_valid  =
  gamble jBlock $ \block ->
  let
    names =
      fmap (AttributeName . Text.pack . ("attribute_" <>) . show)
        [0..Boxed.length (blockTables block) - 1]
  in
    evalExceptT $ do
      striped0 <- hoistEither $ first ppShow $ tableOfBlock (Boxed.fromList names) block

      logical  <- hoistEither $ first (\x -> ppShow striped0 <> "\n" <> ppShow x) $
        Striped.toLogical striped0

      annotate (ppShow logical)
      assert $ Logical.valid logical

prop_logical_from_block :: Property
prop_logical_from_block =
  gamble jBlock $ \block ->
  let
    names =
      fmap (AttributeName . Text.pack . ("attribute_" <>) . show)
        [0..Boxed.length (blockTables block) - 1]
  in
    evalExceptT $ do
      striped0 <- hoistEither $ first ppShow $ tableOfBlock (Boxed.fromList names) block

      logical  <- hoistEither $ first (\x -> ppShow striped0 <> "\n" <> ppShow x) $
        Striped.toLogical striped0

      striped  <- hoistEither $ first ppShow $ Striped.fromLogical (Striped.schema striped0) logical

      annotate (ppShow logical)
      striped0 === striped

tests :: IO Bool
tests =
  checkParallel $$(discover)
