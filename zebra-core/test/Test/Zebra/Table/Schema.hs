{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Table.Schema where

import           Hedgehog
import qualified Hedgehog.Gen as Gen

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import qualified Zebra.Table.Schema as Schema

prop_union_associative :: Property
prop_union_associative = property $ do
  table0    <- forAll jTableSchema
  table1    <- forAll (jExpandedTableSchema table0)
  table2    <- forAll (jContractedTableSchema table0)
  [x, y, z] <- forAll (Gen.shuffle [table0, table1, table2])
  let
    x_yz =
      first (const ())
        (Schema.union x =<< Schema.union y z)

    xy_z =
      first (const ())
        (Schema.union x y >>= \xy -> Schema.union xy z)

    compatible =
      isRight x_yz && isRight xy_z

  cover 90 "compatible schemas" compatible

  x_yz === xy_z

tests :: IO Bool
tests =
  checkParallel $$(discover)
