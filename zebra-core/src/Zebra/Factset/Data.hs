{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
module Zebra.Factset.Data (
    EntityId(..)
  , EntityHash(..)
  , AttributeId(..)
  , AttributeName(..)

  , Time(..)
  , FactsetId(..)
  , Tombstone(..)

  , hashEntityId
  , hashSeed
  , fromDay
  , toDay
  , toUTCTime

  , foreignOfAttributeIds
  , foreignOfTimes
  , foreignOfFactsetIds
  , foreignOfTombstone
  , foreignOfTombstones

  , attributeIdsOfForeign
  , timesOfForeign
  , factsetIdsOfForeign
  , tombstoneOfForeign
  , tombstonesOfForeign
  ) where

import           Anemone.Foreign.Hash (fasthash32')

import           Data.ByteString (ByteString)
import           Data.Thyme (UTCTime, pattern UTCTime, Day, TimeDiff)
import           Data.Thyme.Time.Core (addDays, diffDays, fromGregorian, addUTCTime, fromMicroseconds)
import qualified Data.Vector.Storable as Storable
import           Data.Vector.Unboxed.Deriving (derivingUnbox)
import           Data.Word (Word32, Word64)

import           Foreign.Ptr (castPtr)
import           Foreign.Storable (Storable(..))

import           GHC.Generics (Generic)

import           P

import           X.Text.Show (gshowsPrec)


newtype EntityId =
  EntityId {
      unEntityId :: ByteString
    } deriving (Eq, Ord, Generic)

newtype EntityHash =
  EntityHash {
      unEntityHash :: Word32
    } deriving (Eq, Ord, Generic)

newtype AttributeId =
  AttributeId {
      unAttributeId :: Int64
    } deriving (Eq, Ord, Generic, Storable)

newtype AttributeName =
  AttributeName {
      unAttributeName :: Text
    } deriving (Eq, Ord, Generic)

newtype Time =
  Time {
      unTime :: Int64
    } deriving (Eq, Ord, Enum, Num, Real, Integral, Generic, Storable)

newtype FactsetId =
  FactsetId {
      unFactsetId :: Int64
    } deriving (Eq, Enum, Num, Real, Integral, Generic, Storable)

instance Ord FactsetId where
  compare (FactsetId x) (FactsetId y) =
    compare y x

data Tombstone =
    NotTombstone
  | Tombstone
    deriving (Eq, Ord, Show, Generic)

instance Storable Tombstone where
  sizeOf _ =
    sizeOf (0 :: Int64)

  alignment _ =
    alignment (0 :: Int64)

  peekElemOff p i =
    fmap tombstoneOfForeign $
    peekElemOff (castPtr p) i

  pokeElemOff p i x =
    pokeElemOff (castPtr p) i $
    foreignOfTombstone x

instance Show EntityId where
  showsPrec =
    gshowsPrec

instance Show EntityHash where
  showsPrec =
    gshowsPrec

instance Show AttributeId where
  showsPrec =
    gshowsPrec

instance Show AttributeName where
  showsPrec =
    gshowsPrec

instance Show Time where
  showsPrec =
    gshowsPrec

instance Show FactsetId where
  showsPrec =
    gshowsPrec

hashEntityId :: EntityId -> EntityHash
hashEntityId =
  EntityHash . fasthash32' hashSeed . unEntityId
{-# INLINE hashEntityId #-}

hashSeed :: Word64
hashSeed =
  0xf7a646480e5a3c0f
{-# INLINE hashSeed #-}

fromDay :: Day -> Time
fromDay day =
  Time . fromIntegral $ (diffDays day ivoryEpochDay) * 86400
{-# INLINE fromDay #-}

toDay :: Time -> Day
toDay (Time time) =
  addDays (fromIntegral time `div` 86400) ivoryEpochDay
{-# INLINE toDay #-}

toUTCTime :: Time -> UTCTime
toUTCTime (Time time) =
  addUTCTime (fromSeconds time) ivoryEpochTime
{-# INLINE toUTCTime #-}

ivoryEpochDay :: Day
ivoryEpochDay =
  fromGregorian 1600 3 1
{-# INLINE ivoryEpochDay #-}

ivoryEpochTime :: UTCTime
ivoryEpochTime =
  UTCTime ivoryEpochDay (fromSeconds 0)
{-# INLINE ivoryEpochTime #-}

fromSeconds :: TimeDiff t => Int64 -> t
fromSeconds seconds =
  fromMicroseconds (seconds * 1000000)
{-# INLINE fromSeconds #-}

--
-- We do these conversions here so that we can ensure the 'Storable' instances
-- line up with the conversions.
--

foreignOfAttributeIds :: Storable.Vector AttributeId -> Storable.Vector Int64
foreignOfAttributeIds =
  Storable.unsafeCast
{-# INLINE foreignOfAttributeIds #-}

attributeIdsOfForeign :: Storable.Vector Int64 -> Storable.Vector AttributeId
attributeIdsOfForeign =
  Storable.unsafeCast
{-# INLINE attributeIdsOfForeign #-}

foreignOfTimes :: Storable.Vector Time -> Storable.Vector Int64
foreignOfTimes =
  Storable.unsafeCast
{-# INLINE foreignOfTimes #-}

timesOfForeign :: Storable.Vector Int64 -> Storable.Vector Time
timesOfForeign =
  Storable.unsafeCast
{-# INLINE timesOfForeign #-}

foreignOfFactsetIds :: Storable.Vector FactsetId -> Storable.Vector Int64
foreignOfFactsetIds =
  Storable.unsafeCast
{-# INLINE foreignOfFactsetIds #-}

factsetIdsOfForeign :: Storable.Vector Int64 -> Storable.Vector FactsetId
factsetIdsOfForeign =
  Storable.unsafeCast
{-# INLINE factsetIdsOfForeign #-}

foreignOfTombstone :: Tombstone -> Int64
foreignOfTombstone = \case
  NotTombstone ->
    0
  Tombstone ->
    1
{-# INLINE foreignOfTombstone #-}

tombstoneOfForeign :: Int64 -> Tombstone
tombstoneOfForeign w =
  case w of
    0 ->
      NotTombstone
    _ ->
      Tombstone
{-# INLINE tombstoneOfForeign #-}

foreignOfTombstones :: Storable.Vector Tombstone -> Storable.Vector Int64
foreignOfTombstones =
  Storable.unsafeCast
{-# INLINE foreignOfTombstones #-}

tombstonesOfForeign :: Storable.Vector Int64 -> Storable.Vector Tombstone
tombstonesOfForeign =
  Storable.unsafeCast
{-# INLINE tombstonesOfForeign #-}

derivingUnbox "EntityHash"
  [t| EntityHash -> Word32 |]
  [| unEntityHash |]
  [| EntityHash |]

derivingUnbox "AttributeId"
  [t| AttributeId -> Int64 |]
  [| unAttributeId |]
  [| AttributeId |]

derivingUnbox "Time"
  [t| Time -> Int64 |]
  [| unTime |]
  [| Time |]

derivingUnbox "FactsetId"
  [t| FactsetId -> Int64 |]
  [| unFactsetId |]
  [| FactsetId |]

derivingUnbox "Tombstone"
  [t| Tombstone -> Int64 |]
  [| foreignOfTombstone |]
  [| tombstoneOfForeign |]
