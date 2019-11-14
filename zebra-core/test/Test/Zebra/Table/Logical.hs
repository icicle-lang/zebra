{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Table.Logical where


import           Hedgehog

import qualified Data.Vector as Boxed

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Table.Logical


prop_reversed :: Property
prop_reversed = property $ do
  schema <- forAll jColumnSchema
  x <- forAll (jLogicalValue schema)
  y <- forAll (jLogicalValue schema)

  compare x y ===
    compare (Reversed y) (Reversed x)

prop_nested :: Property
prop_nested = property $ do
  schema <- forAll jColumnSchema
  x <- forAll (jLogicalValue schema)
  y <- forAll (jLogicalValue schema)

  compare x y ===
    compare (Nested (Array (Boxed.singleton x))) (Nested (Array (Boxed.singleton y)))

prop_ord_0 :: Property
prop_ord_0 = withDiscards 1000 . property $ do
  schema <- forAll jColumnSchema
  x <- forAll (jLogicalValue schema)
  y <- forAll (jLogicalValue schema)
  z <- forAll (jLogicalValue schema)

  unless (x > y && y > z)
    discard

  assert $
    x > z

prop_ord_1 :: Property
prop_ord_1 = withDiscards 1000 . property $ do
  schema <- forAll jColumnSchema
  x <- forAll (jLogicalValue schema)
  y <- forAll (jLogicalValue schema)
  z <- forAll (jLogicalValue schema)

  unless (x < y && y < z)
    discard
  assert $
    x < z

tests :: IO Bool
tests =
  checkParallel $$(discover)
