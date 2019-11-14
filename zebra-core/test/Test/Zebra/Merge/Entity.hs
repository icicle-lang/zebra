{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Test.Zebra.Merge.Entity where

import qualified Data.List as List
import qualified Data.Map as Map

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import           Hedgehog.Gen.QuickCheck (arbitrary)
import qualified Hedgehog.Range as Range

import           P

import qualified Prelude as Savage

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Factset.Block
import           Zebra.Factset.Fact
import           Zebra.Merge.Base
import           Zebra.Merge.Entity
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped


fakeBlockId :: BlockDataId
fakeBlockId = BlockDataId 0

entityValuesOfBlock' :: BlockDataId -> Block -> Boxed.Vector EntityValues
entityValuesOfBlock' blockId block = Stream.vectorOfStream $ entityValuesOfBlock blockId block

jColumnSchemas :: Gen [Schema.Column]
jColumnSchemas = Gen.list (Range.linear 0 5) jColumnSchema

blockOfFacts' :: [Schema.Column] -> [Fact] -> Block
blockOfFacts' schemas facts =
  case blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts) of
   Left e -> Savage.error
              ("jBlockFromFacts: invariant failed\n"
              <> "\tgenerated facts cannot be converted to block\n"
              <> "\t" <> show e)
   Right b -> b

prop_entitiesOfBlock_entities :: Property
prop_entitiesOfBlock_entities =
  gamble jYoloBlock $ \block ->
    fmap evEntity (entityValuesOfBlock' fakeBlockId block) === blockEntities block

prop_entitiesOfBlock_indices :: Property
prop_entitiesOfBlock_indices =
  gamble jBlock $ \block ->
    catIndices (entityValuesOfBlock' fakeBlockId block) === takeIndices block
 where
  catIndices evs
   = Boxed.map fst
   $ Boxed.concatMap Boxed.convert
   $ Boxed.concatMap evIndices evs

  takeIndices block
   = Boxed.convert
   $ blockIndices block

prop_entitiesOfBlock_tables_1_entity :: Property
prop_entitiesOfBlock_tables_1_entity = property $ do
  schemas     <- forAll jColumnSchemas
  facts       <- forAll (jFacts schemas)
  (ehash,eid) <- forAll jEntityHashId
  let fixFact f = f { factEntityHash = ehash, factEntityId = eid }
      facts'    = List.sort $ fmap fixFact facts
      block     = blockOfFacts' schemas facts'
      es        = entityValuesOfBlock' fakeBlockId block
  annotate "=== Block ==="
  annotate (ppShow block)
  annotate "=== Entities ==="
  annotate (ppShow es)
  when (null facts) discard

  Boxed.concatMap id (getFakeTableValues es) === blockTables block

getFakeTableValues :: Boxed.Vector EntityValues -> Boxed.Vector (Boxed.Vector Striped.Table)
getFakeTableValues = fmap (fmap (Map.! fakeBlockId) . evTables)

prop_mergeEntityTables_1_block :: Property
prop_mergeEntityTables_1_block =
  gamble jBlock $ \block -> do
  let es = entityValuesOfBlock' fakeBlockId block
      recs_l = mapM mergeEntityTables es

      recs_r = getFakeTableValues es
  annotate "=== Entities ==="
  annotate (ppShow es)
  recs_l === Right recs_r


prop_treeFold_sum :: Property
prop_treeFold_sum =
  gamble arbitrary $ \(bs :: [Int]) ->
  List.sum bs === treeFold (+) 0 id (Boxed.fromList bs)

prop_treeFold_with_map :: Property
prop_treeFold_with_map =
  gamble arbitrary $ \(bs :: [Int]) ->
  List.sum (fmap (+1) bs) === treeFold (+) 0 (+1) (Boxed.fromList bs)


tests :: IO Bool
tests =
  checkParallel $$(discover)
