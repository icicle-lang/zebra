{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Table.Striped where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import           Hedgehog.Gen.QuickCheck (arbitrary)
import qualified Hedgehog.Range as Range

import           Data.Functor.Identity (runIdentity)
import qualified Data.String as String

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Text.Show.Pretty (ppShow)

import qualified Viking.Stream as Stream

import           Control.Monad.Trans.Either (runEitherT)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Data
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped


prop_roundtrip_values :: Property
prop_roundtrip_values = property $ do
  schema   <- forAll jTableSchema
  logical0 <- forAll (jSizedLogical schema)
  evalEither $ do
    striped    <- first ppShow $ Striped.fromLogical schema logical0
    collection <- first ppShow $ Striped.toLogical striped

    unless (logical0 == collection) $
      Left (ppShow striped)

prop_append_array_table :: Property
prop_append_array_table = property $ do
  schema     <- forAll (Schema.Array DenyDefault <$> jColumnSchema)
  logical0   <- forAll (jSizedLogical schema)
  logical1   <- forAll (jSizedLogical schema)
  viaLogical <- either (const discard) pure $
                  Logical.merge logical0 logical1

  evalEither $ do
    striped0   <- first ppShow $ Striped.fromLogical schema logical0
    striped1   <- first ppShow $ Striped.fromLogical schema logical1
    striped    <- first ppShow $ Striped.unsafeAppend striped0 striped1

    viaStriped <- first ppShow $ Striped.toLogical striped

    unless (viaLogical == viaStriped) $
      Left $ String.unlines [
          "== Striped 1 =="
        , (ppShow striped0)
        , "== Striped 2 =="
        , (ppShow striped1)
        , "== Striped Append =="
        , (ppShow striped)
        , "== Striped Append --> Logical =="
        , (ppShow viaStriped)
        , "== Logical Merge =="
        , (ppShow viaLogical)
      ]



prop_roundtrip_splitAt :: Property
prop_roundtrip_splitAt = property $ do
  ix      <- forAll arbitrary
  striped <- forAll jSizedStriped

  tripping striped (Striped.splitAt ix) (uncurry Striped.unsafeAppend)

prop_rechunk :: Property
prop_rechunk = property $ do
  n <-   forAll (Gen.integral (Range.linear 1 100))
  xss <- forAll (Cons.fromNonEmpty <$> Gen.nonEmpty (Range.linear 1 100) jSizedStriped)

  let
    original =
      Striped.unsafeConcat xss

    rechunked =
      bind (Striped.unsafeConcat . Cons.unsafeFromList) $
        runIdentity .
        runEitherT .
        Stream.toList_ .
        Striped.rechunk n $
        Stream.each xss

  original === rechunked


prop_default_table_check :: Property
prop_default_table_check =
  gamble jTableSchema $ \schema -> do
    let
      striped =
        Striped.defaultTable schema
      logical =
        Logical.defaultTable schema

    Striped.toLogical striped === Right logical
    Striped.fromLogical schema logical === Right striped


prop_default_column_check :: Property
prop_default_column_check = property $ do
  n      <- forAll (Gen.int (Range.linear 1 100))
  schema <- forAll jColumnSchema
  let
    striped =
      Striped.defaultColumn n schema
    logical =
      Boxed.replicate n $ Logical.defaultValue schema

  annotate "=== Compare Striped ==="
  Striped.fromValues schema logical === Right striped

  annotate "=== Compare Logical ==="
  Striped.toValues striped === Right logical


prop_transmute_identity :: Property
prop_transmute_identity =
  gamble jSizedStriped $ \table ->
    Striped.transmute (Striped.schema table) table === Right table

prop_transmute_expand :: Property
prop_transmute_expand = property $ do
  table0 <- forAll jSizedStriped
  let
    schema0 =
      Striped.schema table0

  schema <- forAll (jExpandedTableSchema schema0)

  annotate "=== Compare Schema ==="
  (Striped.schema <$> Striped.transmute schema table0) === Right schema

  annotate "=== Roundtrip Table ==="
  (Striped.transmute schema0 =<< Striped.transmute schema table0) === Right table0


prop_transmute_merge :: Property
prop_transmute_merge = property $ do
  schema0  <- forAll jTableSchema
  schema   <- forAll (jExpandedTableSchema schema0)
  logical0 <- forAll (jSizedLogical schema0)
  logical1 <- forAll (jSizedLogical schema0)
  let
    Right striped0 =
      Striped.fromLogical schema0 logical0

    Right striped1 =
      Striped.fromLogical schema0 logical1

  annotate (ppShow striped0)
  annotate (ppShow striped1)

  merge1 <- discardLeft $ Striped.merge striped0 striped1

  tm  <- evalEither $
    Striped.transmute schema merge1

  mtt <- evalEither $ do
    t0 <- Striped.transmute schema striped0
    t1 <- Striped.transmute schema striped1
    Striped.merge t0 t1

  tm === mtt


tests :: IO Bool
tests =
  checkParallel $$(discover)
