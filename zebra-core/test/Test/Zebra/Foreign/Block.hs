{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Foreign.Block where

import qualified Anemone.Foreign.Mempool as Mempool
import           Anemone.Foreign.Segv (withSegv)

import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Trans.Resource (allocate, runResourceT)
import           Control.Monad.Morph (hoist)

import           Data.String (String)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Hedgehog hiding (check)

import           P

import           System.IO (IO)

import           Test.Zebra.Jack
import           Test.Zebra.Util

import           Text.Show.Pretty (ppShow)

import           Control.Monad.Trans.Either (EitherT, pattern EitherT, runEitherT, hoistEither, newEitherT)

import           Zebra.Factset.Block
import           Zebra.Factset.Entity
import qualified Zebra.Factset.Entity as Entity
import           Zebra.Foreign.Block
import           Zebra.Foreign.Entity
import           Zebra.Foreign.Util
import qualified Zebra.Table.Striped as Striped


data CommonError =
    -- Invalid inputs can be wrong in multiple ways at the same, different
    -- backends find different things first, this is ok.
    ExpectedError
  | UnexpectedError String
    deriving (Eq, Show)

fromEntityError :: EntityError -> CommonError
fromEntityError = \case
  EntityAttributeNotFound _ ->
    ExpectedError
  EntityNotEnoughRows ->
    ExpectedError

fromForeignError :: ForeignError -> CommonError
fromForeignError = \case
  ForeignAttributeNotFound ->
    ExpectedError
  ForeignNotEnoughRows ->
    ExpectedError
  err ->
    UnexpectedError $ show err

foreignEntitiesOfBlock' :: Block -> EitherT ForeignError IO (Boxed.Vector Entity)
foreignEntitiesOfBlock' block =
  EitherT $
  runResourceT $ do
    (_, pool) <- allocate Mempool.create Mempool.free
    runEitherT $ do
      fblock    <- hoist liftIO $ foreignOfBlock pool block
      fentities <- hoist liftIO $ foreignEntitiesOfBlock pool fblock
      traverse entityOfForeign fentities

foreignBlockOfEntities' :: Boxed.Vector Entity -> EitherT ForeignError IO (Maybe Block)
foreignBlockOfEntities' entities =
  EitherT .
  runResourceT $ do
    (_, pool) <- allocate Mempool.create Mempool.free
    runEitherT $ do
      fentities <- traverse (foreignOfEntity pool) entities
      fblock    <- foldM (\fb fe -> Just <$> appendEntityToBlock pool fe fb) Nothing fentities
      traverse blockOfForeign fblock

check :: (Eq a, Eq x, Show a, Show x) => String -> a -> Either x a -> PropertyT IO ()
check header v0 ev1 = do
  annotate ("=== " <> header <> " ===")
  annotate "- Original"
  annotate "+ After Conversion"
  case ev1 of
    Left _ ->
      Right v0 === ev1
    Right v1 ->
      v0 === v1

check_entities_of_block :: (Block -> EitherT CommonError IO (Boxed.Vector Entity)) -> Block -> PropertyT IO ()
check_entities_of_block convert block = do
  entities <-
    liftIO $
      withSegv (ppShow block) $
        runEitherT $ convert block

  let
    entityCount :: Int
    entityCount =
      Boxed.length $ blockEntities block

    recordCount :: Int
    recordCount =
      Unboxed.length $ blockIndices block

    sumAttribute :: (Attribute -> Int) -> Boxed.Vector Entity -> Int
    sumAttribute f es =
      Boxed.sum $
      Boxed.map (Boxed.sum . Boxed.map f . Entity.entityAttributes) es

  check "# of entities" entityCount $
    fmap Boxed.length entities

  check "# of time rows" recordCount $
    fmap (sumAttribute $ Storable.length . attributeTime) entities

  check "# of factsetId rows" recordCount $
    fmap (sumAttribute $ Storable.length . attributeFactsetId) entities

  check "# of tombstone rows" recordCount $
    fmap (sumAttribute $ Storable.length . attributeTombstone) entities

  check "# of table rows" recordCount $
    fmap (sumAttribute $ Striped.length . attributeTable) entities


fromEither :: Show x => EitherT x IO a -> PropertyT IO a
fromEither p = do
  e <- liftIO (runEitherT p)
  either (\x -> annotate (ppShow x) >> failure) pure e

check_block_of_entities :: Block -> PropertyT IO ()
check_block_of_entities block = do
  block' <-
    evalExceptT $
      newEitherT $
        liftIO $
          withSegv (ppShow block) $
            runEitherT $ do
              entities <- firstT (\x -> (Boxed.empty, x)) $ foreignEntitiesOfBlock' block
              block'   <- firstT (\x -> (entities, x))    $ foreignBlockOfEntities' entities
              pure block'

  let expect
        | Boxed.null (blockEntities block)
        = Nothing
        | otherwise
        = Just block

  expect === block'


check_c_vs_haskell :: Block -> PropertyT IO ()
check_c_vs_haskell block = do
  entities1 <-
    liftIO $
      withSegv (ppShow block) $
        runEitherT $
        firstT fromForeignError $
        foreignEntitiesOfBlock' block

  let
    entities2 =
      first fromEntityError $
      entitiesOfBlock block

  entities1 === entities2

prop_compare_entities_of_block_valid :: Property
prop_compare_entities_of_block_valid =
  gamble jBlock check_c_vs_haskell

prop_compare_entities_of_block_yolo :: Property
prop_compare_entities_of_block_yolo =
  gamble jYoloBlock check_c_vs_haskell

prop_haskell_entities_of_block :: Property
prop_haskell_entities_of_block =
  gamble jBlock . check_entities_of_block $
    firstT fromEntityError . hoistEither . entitiesOfBlock

prop_c_entities_of_block :: Property
prop_c_entities_of_block =
  gamble jBlock . check_entities_of_block $
    firstT fromForeignError . foreignEntitiesOfBlock'

prop_c_block_of_entities :: Property
prop_c_block_of_entities =
  gamble jBlock $ check_block_of_entities

prop_roundtrip_blocks :: Property
prop_roundtrip_blocks =
  gamble jYoloBlock $ \block ->
    hoist runResourceT $ do
      (_, pool) <- allocate Mempool.create Mempool.free
      trippingIO (liftE . foreignOfBlock pool) blockOfForeign block

return []
tests :: IO Bool
tests =
  checkParallel $$(discover)
