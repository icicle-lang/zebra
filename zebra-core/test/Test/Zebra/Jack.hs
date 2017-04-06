{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Jack (
  -- -- * Zebra.Factset.Block
    jBlock
  , jYoloBlock
  , jBlockEntity
  , jBlockAttribute
  , jBlockIndex
  , jTombstone

  -- * Zebra.Factset.Data
  , jBinaryVersion
  , jEntityId
  , jEntityHashId
  , jAttributeId
  , jAttributeName
  , jTime
  , jDay
  , jFactsetId

  -- * Zebra.Factset.Entity
  , jEntity
  , jAttribute

  -- * Zebra.Factset.Fact
  , jFacts
  , jFact

  -- * Zebra.Table.Schema
  , jField
  , jFieldName
  , jVariant
  , jVariantName

  -- * Zebra.Table.Schema
  , jTableSchema
  , jMapSchema
  , jColumnSchema

  -- * Zebra.Table.Striped
  , jSizedStriped
  , jStriped
  , jStripedArray
  , jStripedColumn

  -- * Zebra.Table.Logical
  , jSizedLogical
  , jLogical
  , jLogicalValue

  , jMaybe'

  -- * Normalization
  , normalizeStriped
  , normalizeLogical
  , normalizeLogicalValue

  -- * x-disorder-jack
  , trippingBoth
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Char as Char
import qualified Data.List as List
import qualified Data.Map as Map
import           Data.Thyme.Calendar (Year, Day, YearMonthDay(..), gregorianValid)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Disorder.Corpus (muppets, southpark, boats, weather)
import           Disorder.Jack (Jack, Property, mkJack, reshrink, shrinkTowards, sized, scale)
import           Disorder.Jack (elements, arbitrary, choose, chooseInt, sizedBounded)
import           Disorder.Jack (oneOf, oneOfRec, listOf, listOfN, vectorOf, justOf, maybeOf)
import           Disorder.Jack ((===), boundedEnum, property, counterexample)

import           P

import qualified Prelude as Savage

import qualified Test.QuickCheck as QC
import           Test.QuickCheck.Instances ()

import           Text.Printf (printf)
import           Text.Show.Pretty (ppShow)

import           Zebra.Factset.Block
import           Zebra.Factset.Data
import           Zebra.Factset.Entity
import           Zebra.Factset.Fact

import           Zebra.Serial.Binary.Data
import           Zebra.Table.Data
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped
import           Zebra.X.Vector.Cons (Cons)
import qualified Zebra.X.Vector.Cons as Cons

------------------------------------------------------------------------

jField :: Jack a -> Jack (Field a)
jField gen =
  Field <$> jFieldName <*> gen

jFieldName :: Jack FieldName
jFieldName =
  FieldName <$> elements boats

jVariant :: Jack a -> Jack (Variant a)
jVariant gen =
  Variant <$> jVariantName <*> gen

jVariantName :: Jack VariantName
jVariantName =
  VariantName <$> elements weather

------------------------------------------------------------------------

tableSchemaTables :: Schema.Table -> [Schema.Table]
tableSchemaTables = \case
  Schema.Binary ->
    []
  Schema.Array x ->
    columnSchemaTables x
  Schema.Map k v ->
    columnSchemaTables k <>
    columnSchemaTables v

tableSchemaColumns :: Schema.Table -> [Schema.Column]
tableSchemaColumns = \case
  Schema.Binary ->
    []
  Schema.Array x ->
    [x]
  Schema.Map k v ->
    [k, v]

columnSchemaTables :: Schema.Column -> [Schema.Table]
columnSchemaTables = \case
  Schema.Unit ->
    []
  Schema.Int ->
    []
  Schema.Double ->
    []
  Schema.Enum variants ->
    concatMap columnSchemaTables . fmap variantData $
      Cons.toList variants
  Schema.Struct fields ->
    concatMap columnSchemaTables . fmap fieldData $
      Cons.toList fields
  Schema.Nested table ->
    [table]
  Schema.Reversed schema ->
    columnSchemaTables schema

columnSchemaColumns :: Schema.Column -> [Schema.Column]
columnSchemaColumns = \case
  Schema.Unit ->
    []
  Schema.Int ->
    []
  Schema.Double ->
    []
  Schema.Enum variants ->
    fmap variantData $ Cons.toList variants
  Schema.Struct fields ->
    fmap fieldData $ Cons.toList fields
  Schema.Nested table ->
    tableSchemaColumns table
  Schema.Reversed schema ->
    [schema]

jTableSchema :: Jack Schema.Table
jTableSchema =
  reshrink tableSchemaTables $
  oneOfRec [
      pure Schema.Binary
    ] [
      Schema.Array <$> jColumnSchema
    , jMapSchema
    ]

jMapSchema :: Jack Schema.Table
jMapSchema =
  Schema.Map <$> jColumnSchema <*> jColumnSchema

jColumnSchema :: Jack Schema.Column
jColumnSchema =
  reshrink columnSchemaColumns $
  oneOfRec [
      pure Schema.Int
    , pure Schema.Double
    ] [
      Schema.Enum <$> smallConsUniqueBy variantName (jVariant jColumnSchema)
    , Schema.Struct <$> smallConsUniqueBy fieldName (jField jColumnSchema)
    , Schema.Nested <$> jTableSchema
    , Schema.Reversed <$> jColumnSchema
    ]

------------------------------------------------------------------------

tableTables :: Striped.Table -> [Striped.Table]
tableTables = \case
  Striped.Binary _ ->
    []
  Striped.Array x ->
    columnTables x
  Striped.Map k v ->
    columnTables k <>
    columnTables v

tableColumns :: Striped.Table -> [Striped.Column]
tableColumns = \case
  Striped.Binary _ ->
    []
  Striped.Array x ->
    [x]
  Striped.Map k v ->
    [k, v]

columnTables :: Striped.Column -> [Striped.Table]
columnTables = \case
  Striped.Unit _ ->
    []
  Striped.Int _ ->
    []
  Striped.Double _ ->
    []
  Striped.Enum _ variants ->
    concatMap columnTables $
      fmap variantData (Cons.toList variants)
  Striped.Struct fields ->
    concatMap columnTables $
      fmap fieldData (Cons.toList fields)
  Striped.Nested _ table ->
    [table]
  Striped.Reversed column ->
    columnTables column

columnColumns :: Striped.Column -> [Striped.Column]
columnColumns = \case
  Striped.Unit _ ->
    []
  Striped.Int _ ->
    []
  Striped.Double _ ->
    []
  Striped.Enum _ variants ->
    fmap variantData $ Cons.toList variants
  Striped.Struct fields ->
    fmap fieldData $ Cons.toList fields
  Striped.Nested _ table ->
    tableColumns table
  Striped.Reversed column ->
    columnColumns column

jSizedStriped :: Jack Striped.Table
jSizedStriped =
  sized $ \size ->
    jStriped =<< chooseInt (0, size `div` 5)

jStriped :: Int -> Jack Striped.Table
jStriped n =
  reshrink tableTables $
  oneOfRec [
      jStripedBinary n
    ] [
      jStripedArray n
    , jStripedMap n
    ]

jByteString :: Int -> Jack ByteString
jByteString n =
  oneOf [
      Char8.pack <$> vectorOf n (fmap Char.chr $ chooseInt (Char.ord 'a', Char.ord 'z'))
    , ByteString.pack <$> vectorOf n boundedEnum
    ]

jStripedBinary  :: Int -> Jack Striped.Table
jStripedBinary n =
  Striped.Binary <$> jByteString n

jStripedArray :: Int -> Jack Striped.Table
jStripedArray n = do
  Striped.Array
    <$> jStripedColumn n

-- FIXME this constructs a corrupt table
jStripedMap :: Int -> Jack Striped.Table
jStripedMap n = do
  Striped.Map
    <$> jStripedColumn n
    <*> jStripedColumn n

jStripedColumn :: Int -> Jack Striped.Column
jStripedColumn n =
  reshrink columnColumns $
  oneOfRec [
      jStripedInt n
    , jStripedDouble n
    ] [
      jStripedEnum n
    , jStripedStruct n
    , jStripedNested n
    , jStripedReversed n
    ]

jStripedInt :: Int -> Jack Striped.Column
jStripedInt n =
  Striped.Int . Storable.fromList <$> vectorOf n sizedBounded

jStripedDouble :: Int -> Jack Striped.Column
jStripedDouble n =
  Striped.Double . Storable.fromList <$> vectorOf n arbitrary

jStripedEnum :: Int -> Jack Striped.Column
jStripedEnum n = do
  sized $ \size -> do
    ntags <- chooseInt (1, 1 + (size `div` 10))
    tags <- Storable.fromList . fmap fromIntegral <$> vectorOf n (chooseInt (0, ntags - 1))
    vs <- Cons.unsafeFromList <$> vectorOf ntags (jVariant . jStripedColumn $ Storable.length tags)
    pure $
      Striped.Enum tags vs

jStripedStruct :: Int -> Jack Striped.Column
jStripedStruct n =
  Striped.Struct <$> smallConsOf (jField (jStripedColumn n))

jStripedNested :: Int -> Jack Striped.Column
jStripedNested n =
  sized $ \size -> do
    ns <- Storable.fromList . fmap fromIntegral <$> vectorOf n (chooseInt (0, size `div` 10))
    Striped.Nested ns <$> jStriped (fromIntegral $ Storable.sum ns)

jStripedReversed :: Int -> Jack Striped.Column
jStripedReversed n =
  Striped.Reversed <$> jStripedColumn n

------------------------------------------------------------------------

jFacts :: [Schema.Column] -> Jack [Fact]
jFacts schemas =
  fmap (List.sort . List.concat) .
  scale (`div` max 1 (length schemas)) $
  zipWithM (\e a -> listOf $ jFact e a) schemas (fmap AttributeId [0..])

jFact :: Schema.Column -> AttributeId -> Jack Fact
jFact schema aid =
  uncurry Fact
    <$> jEntityHashId
    <*> pure aid
    <*> jTime
    <*> jFactsetId
    <*> (strictMaybe <$> maybeOf (jLogicalValue schema))

jSizedLogical :: Schema.Table -> Jack Logical.Table
jSizedLogical schema =
  sized $ \size ->
    jLogical schema =<< chooseInt (0, size `div` 5)

jLogical :: Schema.Table -> Int -> Jack Logical.Table
jLogical tschema n =
  case tschema of
    Schema.Binary ->
      Logical.Binary <$> jByteString n
    Schema.Array x ->
      Logical.Array . Boxed.fromList <$> vectorOf n (jLogicalValue x)
    Schema.Map k v ->
      Logical.Map . Map.fromList <$> vectorOf n (jMapping k v)

jMapping :: Schema.Column -> Schema.Column -> Jack (Logical.Value, Logical.Value)
jMapping k v =
  (,) <$> jLogicalValue k <*> jLogicalValue v

jTag :: Cons Boxed.Vector (Variant a) -> Jack Tag
jTag xs =
  fromIntegral <$> choose (0, Cons.length xs - 1)

jLogicalValue :: Schema.Column -> Jack Logical.Value
jLogicalValue = \case
  Schema.Unit ->
    pure Logical.Unit

  Schema.Int ->
    Logical.Int <$> sizedBounded

  Schema.Double ->
    Logical.Double <$> arbitrary

  Schema.Enum variants -> do
    tag <- jTag variants
    case lookupVariant tag variants of
      Nothing ->
        Savage.error $ renderTagLookupError tag variants
      Just (Variant _ schema) ->
        Logical.Enum tag <$> jLogicalValue schema

  Schema.Struct fields ->
    Logical.Struct <$> traverse (jLogicalValue . fieldData) fields

  Schema.Nested tschema ->
    sized $ \size -> do
      fmap Logical.Nested $ jLogical tschema =<< chooseInt (0, size `div` 10)

  Schema.Reversed schema ->
    Logical.Reversed <$> jLogicalValue schema

renderTagLookupError :: Show a => Tag -> Cons Boxed.Vector (Variant a) -> [Char]
renderTagLookupError tag variants =
  "jLogicalValue: internal error, tag not found" <>
  "\n" <>
  "\n  tag = " <> show tag <>
  "\n" <>
  "\n  variants =" <>
  (List.concatMap ("\n    " <>) . List.lines $ ppShow variants) <>
  "\n"

jBinaryVersion :: Jack BinaryVersion
jBinaryVersion =
  elements [BinaryV2, BinaryV3]

jEntityId :: Jack EntityId
jEntityId =
  let
    mkEnt :: ByteString -> Int -> EntityId
    mkEnt name num =
      EntityId $ name <> Char8.pack (printf "-%03d" num)
  in
    oneOf [
        mkEnt <$> elements southpark <*> pure 0
      , mkEnt <$> elements southpark <*> chooseInt (0, 999)
      ]

jEntityHashId :: Jack (EntityHash, EntityId)
jEntityHashId =
  let
    hash eid =
      EntityHash $ unEntityHash (hashEntityId eid) `mod` 10
  in
    (\eid -> (hash eid, eid)) <$> jEntityId

jAttributeId :: Jack AttributeId
jAttributeId =
  AttributeId <$> choose (0, 10000)

jAttributeName :: Jack AttributeName
jAttributeName =
  AttributeName <$> oneOf [elements muppets, arbitrary]

jTime :: Jack Time
jTime =
  oneOf [
      Time <$> choose (0, 5)
    , fromDay <$> jDay
    ]

jDay :: Jack Day
jDay =
  justOf . fmap gregorianValid $
    YearMonthDay
      <$> jYear
      <*> chooseInt (1, 12)
      <*> chooseInt (1, 31)

jYear :: Jack Year
jYear =
  mkJack (shrinkTowards 2000) $ QC.choose (1600, 3000)

jFactsetId :: Jack FactsetId
jFactsetId =
  oneOf [
      FactsetId <$> choose (0, 5)
    , FactsetId <$> choose (0, 100000)
    ]

jBlock :: Jack Block
jBlock = do
  schemas <- listOfN 0 5 jColumnSchema
  facts <- jFacts schemas
  pure $
    case blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts) of
      Left x ->
        Savage.error $ "Test.Zebra.Jack.jBlock: invariant failed: " <> show x
      Right x ->
        x

-- The blocks generated by this can contain data with broken invariants.
jYoloBlock :: Jack Block
jYoloBlock = do
  sized $ \size ->
    Block
      <$> (Boxed.fromList <$> listOfN 0 (size `div` 5) jBlockEntity)
      <*> (Unboxed.fromList <$> listOfN 0 (size `div` 5) jBlockIndex)
      <*> (Boxed.fromList <$> listOfN 0 (size `div` 5) (jStripedArray =<< chooseInt (0, size `div` 5)))

jBlockEntity :: Jack BlockEntity
jBlockEntity =
  uncurry BlockEntity
    <$> jEntityHashId
    <*> (Unboxed.fromList <$> listOf jBlockAttribute)

jBlockAttribute :: Jack BlockAttribute
jBlockAttribute =
  BlockAttribute
    <$> jAttributeId
    <*> choose (0, 1000000)

jBlockIndex :: Jack BlockIndex
jBlockIndex =
  BlockIndex
    <$> jTime
    <*> jFactsetId
    <*> jTombstone

jEntity :: Jack Entity
jEntity =
  uncurry Entity
    <$> jEntityHashId
    <*> (Boxed.fromList <$> listOf jAttribute)

jAttribute :: Jack Attribute
jAttribute = do
  (ts, ps, bs) <- List.unzip3 <$> listOf ((,,) <$> jTime <*> jFactsetId <*> jTombstone)
  Attribute
    <$> pure (Storable.fromList ts)
    <*> pure (Storable.fromList ps)
    <*> pure (Storable.fromList bs)
    <*> jStripedArray (List.length ts)

jTombstone :: Jack Tombstone
jTombstone =
  elements [
      NotTombstone
    , Tombstone
    ]

jMaybe' :: Jack a -> Jack (Maybe' a)
jMaybe' j =
  oneOfRec [ pure Nothing' ] [ Just' <$> j ]

smallConsOf :: Jack a -> Jack (Cons Boxed.Vector a)
smallConsOf gen =
  sized $ \n ->
    Cons.unsafeFromList <$> listOfN 1 (1 + (n `div` 10)) gen

smallConsUniqueBy :: Ord b => (a -> b) -> Jack a -> Jack (Cons Boxed.Vector a)
smallConsUniqueBy f gen =
  sized $ \n ->
    Cons.unsafeFromList . ordNubBy (comparing f) <$> listOfN 1 (1 + (n `div` 10)) gen

------------------------------------------------------------------------

normalizeStriped :: Striped.Table -> Striped.Table
normalizeStriped table =
  let
    Right x =
      Striped.fromLogical (Striped.schema table) . normalizeLogical =<<
      Striped.toLogical table
  in
    x

normalizeLogical :: Logical.Table -> Logical.Table
normalizeLogical = \case
  Logical.Binary bs ->
    Logical.Binary $ ByteString.sort bs
  Logical.Array xs ->
    Logical.Array . Boxed.fromList . List.sort $ Boxed.toList xs
  Logical.Map kvs ->
    Logical.Map $ fmap normalizeLogicalValue kvs

normalizeLogicalValue :: Logical.Value -> Logical.Value
normalizeLogicalValue = \case
  Logical.Unit ->
    Logical.Unit
  Logical.Int x ->
    Logical.Int x
  Logical.Double x ->
    Logical.Double x
  Logical.Enum tag x ->
    Logical.Enum tag (normalizeLogicalValue x)
  Logical.Struct xs ->
    Logical.Struct $ fmap normalizeLogicalValue xs
  Logical.Nested x ->
    Logical.Nested $ normalizeLogical x
  Logical.Reversed x ->
    Logical.Reversed $ normalizeLogicalValue x

------------------------------------------------------------------------

trippingBoth :: (Monad m, Show (m a), Show (m b), Eq (m a)) => (a -> m b) -> (b -> m a) -> a -> Property
trippingBoth to from x =
  let
    original =
      pure x

    intermediate =
      to x

    roundtrip =
      from =<< intermediate
  in
    counterexample "" .
    counterexample "Roundtrip failed." .
    counterexample "" .
    counterexample "=== Intermediate ===" .
    counterexample (ppShow intermediate) .
    counterexample "" $
      property (original === roundtrip)