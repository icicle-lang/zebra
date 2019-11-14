{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
module Test.Zebra.Jack (
  -- * Zebra.Time
    jTime
  , jDate
  , jTimeOfDay

  -- * Zebra.Factset.Block
  , jBlock
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
  , jFactsetTime
  , jFactsetDay
  , jFactsetId

  -- * Zebra.Factset.Entity
  , jEntity
  , jAttribute

  -- * Zebra.Factset.Fact
  , jFacts
  , jFact

  -- * Zebra.Table.Data
  , jField
  , jFieldName
  , jVariant
  , jVariantName

  -- * Zebra.Table.Schema
  , jTableSchema
  , jMapSchema
  , jColumnSchema
  , tableSchemaV0
  , columnSchemaV0
  , jExpandedTableSchema
  , jExpandedColumnSchema
  , jContractedTableSchema
  , jContractedColumnSchema

  -- * Zebra.Table.Striped
  , jSizedStriped
  , jStriped
  , jStripedArray
  , jStripedColumn

  -- * Zebra.Table.Logical
  , jSizedLogical
  , jSizedLogical1
  , jLogical
  , jLogicalValue

  , jMaybe'

  -- * Normalization
  , normalizeStriped
  , normalizeLogical
  , normalizeLogicalValue

  -- * x-disorder-Gen
  , withList
  , trippingBoth
  , discardLeft
  , gamble
  , vectorOf
  , listOf
  , sizedBounded
  ) where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Char as Char
import           Data.Functor.Identity (Identity(..))
import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Thyme.Calendar as Thyme
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Unboxed as Unboxed

import           Hedgehog
import           Hedgehog.Corpus (muppets, southpark, boats, weather)
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Gen.QuickCheck as Gen
import qualified Hedgehog.Range as Range

import           P

import qualified Prelude as Savage

import           Test.QuickCheck.Instances ()

import           Text.Printf (printf)
import           Text.Show.Pretty (ppShow)

import           Viking (Stream, Of)
import qualified Viking.Stream as Stream

import           Control.Monad.Trans.Either (EitherT, runEitherT)
import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Factset.Block
import qualified Zebra.Factset.Data as Factset
import           Zebra.Factset.Entity
import           Zebra.Factset.Fact
import           Zebra.Serial.Binary.Data
import           Zebra.Table.Data
import qualified Zebra.Table.Encoding as Encoding
import qualified Zebra.Table.Logical as Logical
import qualified Zebra.Table.Schema as Schema
import qualified Zebra.Table.Striped as Striped
import           Zebra.Time

------------------------------------------------------------------------

jDate :: Gen Date
jDate =
  Gen.just (rightToMaybe . fromDays <$> Gen.integral (Range.linear (toDays minBound) (toDays maxBound)))

jTime :: Gen Time
jTime =
  Gen.just (rightToMaybe . fromMicroseconds <$> Gen.integral (Range.linear  (toMicroseconds minBound) (toMicroseconds maxBound)))

jTimeOfDay :: Gen TimeOfDay
jTimeOfDay =
  toTimeOfDay <$> Gen.integral (Range.linear 0 (24 * 60 * 60 * 1000000))

------------------------------------------------------------------------

jField :: Gen a -> Gen (Field a)
jField gen =
  Field <$> jFieldName <*> gen

jFieldName :: Gen FieldName
jFieldName =
  FieldName <$> Gen.element boats

jVariant :: Gen a -> Gen (Variant a)
jVariant gen =
  Variant <$> jVariantName <*> gen

jVariantName :: Gen VariantName
jVariantName =
  VariantName <$> Gen.element weather

------------------------------------------------------------------------

tableSchemaTables :: Schema.Table -> [Schema.Table]
tableSchemaTables = \case
  Schema.Binary _ _ ->
    []
  Schema.Array _ x ->
    columnSchemaTables x
  Schema.Map _ k v ->
    columnSchemaTables k <>
    columnSchemaTables v

tableSchemaColumns :: Schema.Table -> [Schema.Column]
tableSchemaColumns = \case
  Schema.Binary _ _ ->
    []
  Schema.Array _ x ->
    [x]
  Schema.Map _ k v ->
    [k, v]

columnSchemaTables :: Schema.Column -> [Schema.Table]
columnSchemaTables = \case
  Schema.Unit ->
    []
  Schema.Int _ _ ->
    []
  Schema.Double _ ->
    []
  Schema.Enum _ variants ->
    concatMap columnSchemaTables . fmap variantData $
      Cons.toList variants
  Schema.Struct _ fields ->
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
  Schema.Int _ _ ->
    []
  Schema.Double _ ->
    []
  Schema.Enum _ variants ->
    fmap variantData $ Cons.toList variants
  Schema.Struct _ fields ->
    fmap fieldData $ Cons.toList fields
  Schema.Nested table ->
    tableSchemaColumns table
  Schema.Reversed schema ->
    [schema]

-- Strip a schema of features which can't be used in SchemaV0.
tableSchemaV0 :: Schema.Table -> Schema.Table
tableSchemaV0 = \case
  Schema.Binary _ _ ->
    Schema.Binary DenyDefault Encoding.Binary
  Schema.Array _ x ->
    Schema.Array DenyDefault $ columnSchemaV0 x
  Schema.Map _ k v ->
    Schema.Map DenyDefault (columnSchemaV0 k) (columnSchemaV0 v)

columnSchemaV0 :: Schema.Column -> Schema.Column
columnSchemaV0 = \case
  Schema.Unit ->
    Schema.Unit
  Schema.Int _ _ ->
    Schema.Int DenyDefault Encoding.Int
  Schema.Double _ ->
    Schema.Double DenyDefault
  Schema.Enum _ variants ->
    Schema.Enum DenyDefault $ fmap (fmap columnSchemaV0) variants
  Schema.Struct _ fields ->
    Schema.Struct DenyDefault $ fmap (fmap columnSchemaV0) fields
  Schema.Nested table ->
    Schema.Nested $ tableSchemaV0 table
  Schema.Reversed schema ->
    Schema.Reversed $ columnSchemaV0 schema

jTableSchema :: Gen Schema.Table
jTableSchema =
  Gen.shrink tableSchemaTables $
  Gen.recursive Gen.choice [
      Schema.Binary <$> jDefault <*> jBinaryEncoding
    ] [
      Schema.Array <$> jDefault <*> jColumnSchema
    , jMapSchema
    ]

jDefault :: Gen Default
jDefault =
  Gen.element [
      DenyDefault
    , AllowDefault
    ]

jBinaryEncoding :: Gen Encoding.Binary
jBinaryEncoding =
  Gen.element [
      Encoding.Binary
    , Encoding.Utf8
    ]

jMapSchema :: Gen Schema.Table
jMapSchema =
  Schema.Map <$> jDefault <*> jColumnSchema <*> jColumnSchema

jColumnSchema :: Gen Schema.Column
jColumnSchema =
  Gen.shrink columnSchemaColumns $
  Gen.recursive Gen.choice [
      Schema.Int <$> jDefault <*> jIntEncoding
    , Schema.Double <$> jDefault
    ] [
      Schema.Enum <$> jDefault <*> smallConsUniqueBy variantName (jVariant jColumnSchema)
    , Schema.Struct <$> jDefault <*> smallConsUniqueBy fieldName (jField jColumnSchema)
    , Schema.Nested <$> jTableSchema
    , Schema.Reversed <$> jColumnSchema
    ]

jIntEncoding :: Gen Encoding.Int
jIntEncoding =
  Gen.element [
      Encoding.Int
    , Encoding.Date
    , Encoding.TimeSeconds
    , Encoding.TimeMilliseconds
    , Encoding.TimeMicroseconds
    ]

------------------------------------------------------------------------

jExpandedTableSchema :: Schema.Table -> Gen Schema.Table
jExpandedTableSchema = \case
  Schema.Binary def encoding ->
    pure $ Schema.Binary def encoding
  Schema.Array def x ->
    Schema.Array def <$> jExpandedColumnSchema x
  Schema.Map def k v ->
    Schema.Map def k <$> jExpandedColumnSchema v

jExpandedColumnSchema :: Schema.Column -> Gen Schema.Column
jExpandedColumnSchema = \case
  Schema.Unit ->
    pure Schema.Unit
  Schema.Int def encoding ->
    pure $ Schema.Int def encoding
  Schema.Double def ->
    pure $ Schema.Double def
  Schema.Enum def vs ->
    Schema.Enum def <$> traverse (traverse jExpandedColumnSchema) vs
  Schema.Struct def fs0 -> do
    fs1 <- Cons.toList <$> traverse (traverse jExpandedColumnSchema) fs0
    fs2 <- fmap2 (fmap (Schema.withDefaultColumn AllowDefault)) <$> Gen.list (Range.linear 0 3) $ jField jColumnSchema

    let
      fs3 =
        ordNubBy (comparing fieldName) (fs1 <> fs2)

    fs4 <- Gen.shuffle fs3
    pure . Schema.Struct def $ Cons.unsafeFromList fs4
  Schema.Nested x ->
    Schema.Nested <$> jExpandedTableSchema x
  Schema.Reversed x ->
    Schema.Reversed <$> jExpandedColumnSchema x

jContractedTableSchema :: Schema.Table -> Gen Schema.Table
jContractedTableSchema = \case
  Schema.Binary def encoding ->
    pure $ Schema.Binary def encoding
  Schema.Array def x ->
    Schema.Array def <$> jContractedColumnSchema x
  Schema.Map def k v ->
    Schema.Map def k <$> jContractedColumnSchema v

jContractedColumnSchema :: Schema.Column -> Gen Schema.Column
jContractedColumnSchema = \case
  Schema.Unit ->
    pure Schema.Unit
  Schema.Int def encoding ->
    pure $ Schema.Int def encoding
  Schema.Double def ->
    pure $ Schema.Double def
  Schema.Enum def vs ->
    Schema.Enum def <$> traverse (traverse jContractedColumnSchema) vs
  Schema.Struct def fs0 -> do
    fs1 <- Cons.toList <$> traverse (traverse jContractedColumnSchema) fs0
    fs2 <- Gen.filter (not . null) $ Gen.subsequence fs1
    pure . Schema.Struct def $ Cons.unsafeFromList fs2
  Schema.Nested x ->
    Schema.Nested <$> jContractedTableSchema x
  Schema.Reversed x ->
    Schema.Reversed <$> jContractedColumnSchema x

------------------------------------------------------------------------

tableTables :: Striped.Table -> [Striped.Table]
tableTables = \case
  Striped.Binary _ _ _ ->
    []
  Striped.Array _ x ->
    columnTables x
  Striped.Map _ k v ->
    columnTables k <>
    columnTables v

tableColumns :: Striped.Table -> [Striped.Column]
tableColumns = \case
  Striped.Binary _ _ _ ->
    []
  Striped.Array _ x ->
    [x]
  Striped.Map _ k v ->
    [k, v]

columnTables :: Striped.Column -> [Striped.Table]
columnTables = \case
  Striped.Unit _ ->
    []
  Striped.Int _ _ _ ->
    []
  Striped.Double _ _ ->
    []
  Striped.Enum _ _ variants ->
    concatMap columnTables $
      fmap variantData (Cons.toList variants)
  Striped.Struct _ fields ->
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
  Striped.Int _ _ _ ->
    []
  Striped.Double _ _ ->
    []
  Striped.Enum _ _ variants ->
    fmap variantData $ Cons.toList variants
  Striped.Struct _ fields ->
    fmap fieldData $ Cons.toList fields
  Striped.Nested _ table ->
    tableColumns table
  Striped.Reversed column ->
    columnColumns column

jSizedStriped :: Gen Striped.Table
jSizedStriped =
  Gen.sized $ \size ->
    jStriped =<< Gen.integral (Range.linear 0 (unSize size `div` 5))

jStriped :: Int -> Gen Striped.Table
jStriped n =
  Gen.shrink tableTables $
  Gen.recursive Gen.choice [
      jStripedBinary n
    ] [
      jStripedArray n
    , jStripedMap n
    ]

jByteString :: Int -> Gen ByteString
jByteString n =
  Gen.choice [
      Char8.pack
        <$> vectorOf n (fmap Char.chr $ Gen.int (Range.linear (Char.ord 'a') (Char.ord 'z')))
    , ByteString.pack
        <$> vectorOf n Gen.enumBounded
    ]

jUtf8 :: Int -> Gen ByteString
jUtf8 n =
  fmap (fixupUtf8 n) $
  Gen.choice [
      vectorOf n (fmap Char.chr $ Gen.int (Range.linear (Char.ord 'a') (Char.ord 'z')))
    , vectorOf n (fmap Char.chr $ Gen.int (Range.linear (Char.ord minBound) (Char.ord maxBound)))
    ]

fixupUtf8 :: Int -> [Char] -> ByteString
fixupUtf8 n xs =
  let
    bs =
      Text.encodeUtf8 $ Text.pack xs

    m =
      ByteString.length bs
  in
    if m > n then
      fixupUtf8 n $ Savage.init xs -- sorry
    else
      -- pad with trash to make exactly 'n' bytes
      bs <> Char8.replicate (n - m) 'x'

jStripedBinary  :: Int -> Gen Striped.Table
jStripedBinary n = do
  encoding <- jBinaryEncoding
  case encoding of
    Encoding.Binary ->
      Striped.Binary <$> jDefault <*> pure encoding <*> jByteString n
    Encoding.Utf8 ->
      -- FIXME This will work out strangely for tests that have nested binary
      -- FIXME as we might get nonsense when we slice a utf-8 string, but the
      -- FIXME tests which generate striped tables don't care at the moment.
      Striped.Binary <$> jDefault <*> pure encoding <*> jUtf8 n

jStripedArray :: Int -> Gen Striped.Table
jStripedArray n = do
  Striped.Array
    <$> jDefault
    <*> jStripedColumn n

-- FIXME this constructs a corrupt table
jStripedMap :: Int -> Gen Striped.Table
jStripedMap n = do
  Striped.Map
    <$> jDefault
    <*> jStripedColumn n
    <*> jStripedColumn n

jStripedColumn :: Int -> Gen Striped.Column
jStripedColumn n =
  Gen.shrink columnColumns $
  Gen.recursive Gen.choice [
      jStripedInt n
    , jStripedDouble n
    ] [
      jStripedEnum n
    , jStripedStruct n
    , jStripedNested n
    , jStripedReversed n
    ]

jStripedInt :: Int -> Gen Striped.Column
jStripedInt n =
  Striped.Int
    <$> jDefault
    <*> jIntEncoding
    <*> (Storable.fromList <$> vectorOf n (Gen.integral (Range.linear minBound maxBound)))

jStripedDouble :: Int -> Gen Striped.Column
jStripedDouble n =
  Striped.Double
    <$> jDefault
    <*> (Storable.fromList <$> vectorOf n Gen.arbitrary)

jStripedEnum :: Int -> Gen Striped.Column
jStripedEnum n = do
  Gen.sized $ \size -> do
    ntags <- Gen.integral (Range.linear 1 (1 + (unSize size `div` 10)))
    tags <- Storable.fromList . fmap fromIntegral <$> vectorOf n (Gen.int (Range.linear 0 (ntags - 1)))
    vs <- Cons.unsafeFromList <$> vectorOf ntags (jVariant . jStripedColumn $ Storable.length tags)
    def <- jDefault
    pure $
      Striped.Enum def tags vs

jStripedStruct :: Int -> Gen Striped.Column
jStripedStruct n =
  Striped.Struct
    <$> jDefault
    <*> smallConsUniqueBy fieldName (jField (jStripedColumn n))

jStripedNested :: Int -> Gen Striped.Column
jStripedNested n =
  Gen.sized $ \size -> do
    ns <- Storable.fromList . fmap fromIntegral <$> vectorOf n (Gen.integral (Range.linear 0 (size `div` 10)))
    Striped.Nested ns <$> jStriped (fromIntegral $ Storable.sum ns)

jStripedReversed :: Int -> Gen Striped.Column
jStripedReversed n =
  Striped.Reversed <$> jStripedColumn n

------------------------------------------------------------------------

jFacts :: [Schema.Column] -> Gen [Fact]
jFacts schemas =
  fmap (List.sort . List.concat) .
  Gen.scale (`div` max 1 (Size (length schemas))) $
  zipWithM (\e a -> Gen.list (Range.linear 0 100) $ jFact e a) schemas (fmap Factset.AttributeId [0..])

jFact :: Schema.Column -> Factset.AttributeId -> Gen Fact
jFact schema aid =
  uncurry Fact
    <$> jEntityHashId
    <*> pure aid
    <*> jFactsetTime
    <*> jFactsetId
    <*> (strictMaybe <$> Gen.maybe (jLogicalValue schema))

jSizedLogical :: Schema.Table -> Gen Logical.Table
jSizedLogical schema =
  Gen.sized $ \size ->
    jLogical schema =<< Gen.int (Range.linear 0 (unSize size `div` 5))

jSizedLogical1 :: Schema.Table -> Gen Logical.Table
jSizedLogical1 schema =
  Gen.sized $ \size ->
    jLogical schema =<< Gen.int (Range.linear 1 (max 1 (unSize size `div` 5)))

jLogical :: Schema.Table -> Int -> Gen Logical.Table
jLogical tschema n =
  case tschema of
    Schema.Binary _ Encoding.Binary ->
      Logical.Binary <$> jByteString n
    Schema.Binary _ Encoding.Utf8 ->
      Logical.Binary <$> jUtf8 n
    Schema.Array _ x ->
      Logical.Array . Boxed.fromList <$> vectorOf n (jLogicalValue x)
    Schema.Map _ k v ->
      Logical.Map . Map.fromList <$> vectorOf n (jMapping k v)

jMapping :: Schema.Column -> Schema.Column -> Gen (Logical.Value, Logical.Value)
jMapping k v =
  (,) <$> jLogicalValue k <*> jLogicalValue v

jTag :: Cons Boxed.Vector (Variant a) -> Gen Tag
jTag xs =
  fromIntegral <$> Gen.integral (Range.linear 0 (Cons.length xs - 1))

jLogicalValue :: Schema.Column -> Gen Logical.Value
jLogicalValue = \case
  Schema.Unit ->
    pure Logical.Unit

  Schema.Int _ Encoding.Int ->
    Logical.Int <$> Gen.integral (Range.linear minBound maxBound)

  Schema.Int _ Encoding.Date ->
    Logical.Int . Encoding.encodeDate <$> jDate

  Schema.Int _ Encoding.TimeSeconds ->
    Logical.Int . Encoding.encodeTimeSeconds <$> jTime

  Schema.Int _ Encoding.TimeMilliseconds ->
    Logical.Int . Encoding.encodeTimeMilliseconds <$> jTime

  Schema.Int _ Encoding.TimeMicroseconds ->
    Logical.Int . Encoding.encodeTimeMicroseconds <$> jTime

  Schema.Double _ ->
    Logical.Double <$> Gen.arbitrary

  Schema.Enum _ variants -> do
    tag <- jTag variants
    case lookupVariant tag variants of
      Nothing ->
        Savage.error $ renderTagLookupError tag variants
      Just (Variant _ schema) ->
        Logical.Enum tag <$> jLogicalValue schema

  Schema.Struct _ fields ->
    Logical.Struct <$> traverse (jLogicalValue . fieldData) fields

  Schema.Nested tschema ->
    Gen.sized $ \size -> do
      fmap Logical.Nested $ jLogical tschema =<< Gen.int (Range.linear 0 (unSize size `div` 10))

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

jBinaryVersion :: Gen BinaryVersion
jBinaryVersion =
  Gen.element [BinaryV2, BinaryV3]

jEntityId :: Gen Factset.EntityId
jEntityId =
  let
    mkEnt :: ByteString -> Int -> Factset.EntityId
    mkEnt name num =
      Factset.EntityId $ name <> Char8.pack (printf "-%03d" num)
  in
    Gen.choice [
        mkEnt <$> Gen.element southpark <*> pure 0
      , mkEnt <$> Gen.element southpark <*> Gen.int (Range.linear 0 999)
      ]

jEntityHashId :: Gen (Factset.EntityHash, Factset.EntityId)
jEntityHashId =
  let
    hash eid =
      Factset.EntityHash $
        Factset.unEntityHash (Factset.hashEntityId eid) `mod` 10
  in
    (\eid -> (hash eid, eid)) <$> jEntityId

jAttributeId :: Gen Factset.AttributeId
jAttributeId =
  Factset.AttributeId <$> Gen.integral (Range.linear 0 10000)

jAttributeName :: Gen Factset.AttributeName
jAttributeName =
  Factset.AttributeName <$> Gen.choice [Gen.element muppets, Gen.arbitrary]

jFactsetTime :: Gen Factset.Time
jFactsetTime =
  Gen.choice [
      Factset.Time <$> Gen.integral (Range.linear 0 5)
    , Factset.fromDay <$> jFactsetDay
    ]

jFactsetDay :: Gen Thyme.Day
jFactsetDay =
  Gen.just . fmap Thyme.gregorianValid $
    Thyme.YearMonthDay
      <$> jFactsetYear
      <*> Gen.int (Range.linear 1 12)
      <*> Gen.int (Range.linear 1 31)

jFactsetYear :: Gen Thyme.Year
jFactsetYear =
  Gen.integral (Range.linearFrom 2000 1600 3000)

jFactsetId :: Gen Factset.FactsetId
jFactsetId =
  Gen.choice [
      Factset.FactsetId <$> Gen.integral (Range.linear 0 5)
    , Factset.FactsetId <$> Gen.integral (Range.linear 0 100000)
    ]

jBlock :: Gen Block
jBlock = do
  schemas <- Gen.list (Range.linear 0 5) jColumnSchema
  facts <- jFacts schemas
  pure $
    case blockOfFacts (Boxed.fromList schemas) (Boxed.fromList facts) of
      Left x ->
        Savage.error $ "Test.Zebra.Gen.jBlock: invariant failed: " <> show x
      Right x ->
        x

-- The blocks generated by this can contain data with broken invariants.
jYoloBlock :: Gen Block
jYoloBlock = do
  Gen.sized $ \size ->
    Block
      <$> (Boxed.fromList <$> Gen.list (Range.linear 0 (unSize size `div` 5)) jBlockEntity)
      <*> (Unboxed.fromList <$> Gen.list (Range.linear 0 (unSize size `div` 5)) jBlockIndex)
      <*> (Boxed.fromList <$> Gen.list (Range.linear 0 (unSize size `div` 5))
            (fmap (Striped.Array DenyDefault) . jStripedColumn =<< Gen.int (Range.linear 0 (unSize size `div` 5))))

jBlockEntity :: Gen BlockEntity
jBlockEntity =
  uncurry BlockEntity
    <$> jEntityHashId
    <*> (Unboxed.fromList <$> Gen.list (Range.linear 0 100) jBlockAttribute)

jBlockAttribute :: Gen BlockAttribute
jBlockAttribute =
  BlockAttribute
    <$> jAttributeId
    <*> Gen.integral (Range.linear 0 1000000)

jBlockIndex :: Gen BlockIndex
jBlockIndex =
  BlockIndex
    <$> jFactsetTime
    <*> jFactsetId
    <*> jTombstone

jEntity :: Gen Entity
jEntity =
  uncurry Entity
    <$> jEntityHashId
    <*> (Boxed.fromList <$> Gen.list (Range.linear 0 100) jAttribute)

jAttribute :: Gen Attribute
jAttribute = do
  (ts, ps, bs) <- List.unzip3 <$> Gen.list (Range.linear 0 100) ((,,) <$> jFactsetTime <*> jFactsetId <*> jTombstone)
  Attribute
    <$> pure (Storable.fromList ts)
    <*> pure (Storable.fromList ps)
    <*> pure (Storable.fromList bs)
    <*> jStripedArray (List.length ts)

jTombstone :: Gen Tombstone
jTombstone =
  Gen.element [
      NotTombstone
    , Tombstone
    ]

jMaybe' :: Gen a -> Gen (Maybe' a)
jMaybe' j =
  Gen.recursive Gen.choice [ pure Nothing' ] [ Just' <$> j ]

smallConsUniqueBy :: Ord b => (a -> b) -> Gen a -> Gen (Cons Boxed.Vector a)
smallConsUniqueBy f gen =
  Gen.sized $ \size ->
    Cons.unsafeFromList . ordNubBy (comparing f) <$> Gen.list (Range.linear 1  (1 + (unSize size `div` 10))) gen

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

trippingBoth ::
     (MonadTest m, Monad f, Show b, Show (f a), Eq (f a))
  => a
  -> (a -> f b)
  -> (b -> f a)
  -> m ()
trippingBoth x encode decode = do
  let
    mx =
      pure x

    my =
      encode x >>= decode

  annotate $ Savage.unlines [
      "━━━ Original ━━━"
    , show mx
    , "━━━ Roundtrip ━━━"
    , show my
    ]

  mx === my

gamble :: Show a => Gen a -> (a -> PropertyT Savage.IO ()) -> Property
gamble gen prop =
  property $
    forAll gen >>= prop

withList :: (Stream (Of a) Identity () -> Stream (Of b) (EitherT x Identity) ()) -> [a] -> Either x [b]
withList f =
  runIdentity . runEitherT . Stream.toList_ . f . Stream.each

discardLeft :: Monad m => Either x a -> PropertyT m a
discardLeft = \case
  Left _ ->
    discard
  Right a ->
    pure a

vectorOf :: Int -> Gen a -> Gen [a]
vectorOf = Gen.list . Range.singleton

listOf :: Gen a -> Gen [a]
listOf = Gen.list (Range.linear 0 100)

sizedBounded :: (Bounded a, Integral a) => Gen a
sizedBounded =
  Gen.integral (Range.linear minBound maxBound)
