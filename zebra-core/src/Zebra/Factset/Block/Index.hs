{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Zebra.Factset.Block.Index (
    BlockIndex(..)
  , Tombstone(..)
  , indicesOfFacts
  ) where

import           Data.Vector.Unboxed.Deriving (derivingUnbox)

import           GHC.Generics (Generic)
import           P

import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Unboxed as Unboxed

import           Zebra.Factset.Data
import           Zebra.Factset.Fact


-- FIXME Might be good if this were using 3x Storable.Vector instead of a
-- FIXME single Unboxed.Vector, as it would make translation to C smoother.
data BlockIndex =
  BlockIndex {
      indexTime :: !Time
    , indexFactsetId :: !FactsetId
    , indexTombstone :: !Tombstone
    } deriving (Eq, Ord, Show, Generic)

derivingUnbox "BlockIndex"
  [t| BlockIndex -> (Time, FactsetId, Tombstone) |]
  [| \(BlockIndex x y z) -> (x, y, z) |]
  [| \(x, y, z) -> BlockIndex x y z |]

indicesOfFacts :: Boxed.Vector Fact -> Unboxed.Vector BlockIndex
indicesOfFacts =
  let
    fromFact :: Fact -> BlockIndex
    fromFact fact =
      BlockIndex
        (factTime fact)
        (factFactsetId fact)
        (maybe' Tombstone (const NotTombstone) $ factValue fact)
  in
    Unboxed.convert . fmap fromFact