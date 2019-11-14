{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Factset.Data where

import           P

import           Hedgehog

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Factset.Data
import           Zebra.Foreign.Bindings (pattern C'ZEBRA_HASH_SEED)


prop_roundtrip_day :: Property
prop_roundtrip_day =
  gamble jFactsetDay $
    tripping `flip` fromDay `flip` (Just . toDay)

prop_hash_seed :: Property
prop_hash_seed =
  withTests 1 . property $
    hashSeed === C'ZEBRA_HASH_SEED

tests :: IO Bool
tests =
  checkParallel $$(discover)
