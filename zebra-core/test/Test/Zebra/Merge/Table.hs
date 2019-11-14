{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Merge.Table where

import           Data.Functor.Identity (runIdentity)
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.String (String)
import qualified Data.Vector as Boxed
import qualified Data.Map as Map

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import           Hedgehog.Gen.QuickCheck (arbitrary)
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import           Viking.Stream (Of(..))
import qualified Viking.Stream as Stream

import           Control.Monad.Trans.Either (runEitherT)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Merge.Table (UnionTableError(..))
import qualified Zebra.Merge.Table as Merge
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError(..))
import qualified Zebra.Table.Striped as Striped
import qualified Zebra.Table.Logical as Logical

jFileTable :: Schema.Table -> Gen Striped.Table
jFileTable schema = do
  Gen.sized $ \size -> do
    n <- Gen.int (Range.linear 0 (20 * (unSize size `div` 5)))
    x <- Striped.fromLogical schema <$> jLogical schema n
    either (const Gen.discard) pure x

jSplits :: Striped.Table -> Gen (NonEmpty Striped.Table)
jSplits x =
  let
    n =
      Striped.length x
  in
    if n == 0 then
      pure $ x :| []
    else do
      ix <- Gen.int (Range.linear 1 (min n 20))

      let
        (y, z) =
          Striped.splitAt ix x

      ys <- jSplits z
      pure $ y :| toList ys

jFile :: Schema.Table -> Gen (NonEmpty Striped.Table)
jFile schema = do
  jSplits =<< jFileTable schema

jModSchema :: Schema.Table -> Gen Schema.Table
jModSchema schema =
  Gen.choice [
      pure schema
    , jExpandedTableSchema schema
    , jContractedTableSchema schema
    ]

unionSimple :: Cons Boxed.Vector (NonEmpty Striped.Table) -> Either String (Maybe Striped.Table)
unionSimple xss0 =
  case Striped.merges =<< traverse Striped.merges (fmap Cons.fromNonEmpty xss0) of
    Left (StripedLogicalMergeError _) ->
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right x ->
      pure $ pure x

unionList ::
      Maybe Merge.MaximumRowSize
   -> Cons Boxed.Vector (NonEmpty Striped.Table)
   -> Either String (Maybe Striped.Table)
unionList msize xss0 =
  case runIdentity . runEitherT . Stream.toList . Merge.unionStriped msize $ fmap Stream.each xss0 of
    Left (UnionLogicalMergeError _) ->
      pure Nothing
    Left err ->
      Left $ ppShow err
    Right (xs0 :> ()) ->
      case Cons.fromList xs0 of
        Nothing ->
          Left "Union returned empty stream"
        Just xs ->
          case Striped.unsafeConcat xs of
            Left err ->
              Left $ ppShow err
            Right x ->
              pure $ pure x

prop_union_identity :: Property
prop_union_identity = property $ do
  schema <- forAll jMapSchema
  file0  <- forAll (jFile schema)
  either (\x -> annotate x >> failure) (const success) $ do
    let
      files =
        Cons.from2 file0 (Striped.empty schema :| [])

      Right file =
        Striped.unsafeConcat $
        Cons.fromNonEmpty file0

    x <- first ppShow $ unionList Nothing files
    pure $
      Just (normalizeStriped file)
      ==
      fmap normalizeStriped x

prop_union_files_same_schema :: Property
prop_union_files_same_schema = property $ do
  schema <- forAll jMapSchema
  files  <- forAll (Cons.unsafeFromList <$> Gen.list (Range.linear 1 10) (jFile schema))

  either (\x -> annotate x >> failure) (const success) $ do
    x <- first ppShow $ unionSimple files
    y <- first ppShow $ unionList Nothing files
    pure $
      fmap normalizeStriped x
      ==
      fmap normalizeStriped y

prop_union_files_empty :: Property
prop_union_files_empty = property $ do
  schema  <- forAll jMapSchema
  files   <- forAll (Cons.unsafeFromList <$> Gen.list (Range.linear 1 10) (jFile schema))

  either (\x -> annotate x >> failure) (const success) $ do
    x <- first ppShow $ unionList (Just (Merge.MaximumRowSize (-1))) files
    pure $
      Just (Striped.empty schema) == x

prop_union_files_diff_schema :: Property
prop_union_files_diff_schema = property $ do
  schema  <- forAll jMapSchema
  schemas <- forAll (Cons.unsafeFromList <$> Gen.list (Range.linear 1 10) (jModSchema schema))

  when (isLeft (Cons.fold1M' Schema.union schemas)) discard

  files <- forAll (traverse jFile schemas)

  either (\x -> annotate x >> failure) (const success) $ do
    x <- first ppShow $ unionSimple files
    y <- first ppShow $ unionList Nothing files
    pure $
      fmap normalizeStriped x
      ==
      fmap normalizeStriped y

prop_union_with_max_is_submap :: Property
prop_union_with_max_is_submap = property $ do
  schema <- forAll jMapSchema
  files  <- forAll (Cons.unsafeFromList <$> Gen.list (Range.linear 1 10) (jFile schema))
  msize  <- forAll (Gen.integral (Range.linear (-1) 100))
  either (\x -> annotate x >> failure) (const success) $ do
    x0 <- first ppShow $ unionList (Just (Merge.MaximumRowSize msize)) files
    y0 <- first ppShow $ unionSimple files
    ok <- for (liftA2 (,) x0 y0) $ \(x1, y1) -> do
      x2 <- first ppShow . Logical.takeMap =<< first ppShow (Striped.toLogical x1)
      y2 <- first ppShow . Logical.takeMap =<< first ppShow (Striped.toLogical y1)
      return $ x2 `Map.isSubmapOf` y2

    return $ maybe True id ok


tests :: IO Bool
tests =
  checkParallel $$(discover)
