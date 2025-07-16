{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Serial.Json.Util (
    encodeJson
  , encodeJsonIndented
  , decodeJson

  , JsonDecodeError(..)
  , renderJsonDecodeError

  -- * Parsing
  , pText
  , pBinary
  , pUnit
  , pInt
  , pDate
  , pTime
  , pDouble
  , pEnum
  , withStructField
  , withOptionalField
  , kmapM
  , fromEither

  -- * Pretty Printing
  , ppText
  , ppBinary
  , ppUnit
  , ppInt
  , ppDate
  , ppTime
  , ppDouble
  , ppEnum
  , ppStruct
  , ppStructField
  ) where

import           Data.Aeson ((.=), (.:), (.:?))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Encode.Pretty as Aeson
import           Data.Aeson ((<?>))
-- import qualified Data.Aeson.Internal as Aeson
import qualified Data.Aeson.Parser as Aeson
import qualified Data.Aeson.Types as Aeson
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Base64 as Base64
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.List as List
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Vector as Boxed

import           P

import           Zebra.Table.Data
import           Zebra.Time
import qualified Data.Aeson.KeyMap as KeyMap
import           Data.Aeson.Key (toText, toString, fromText)


data JsonDecodeError =
    JsonDecodeError !Aeson.JSONPath !Text
    deriving (Eq, Show)

renderJsonDecodeError :: JsonDecodeError -> Text
renderJsonDecodeError = \case
  JsonDecodeError path msg ->
    Text.pack . Aeson.formatError path $ Text.unpack msg

encodeJson :: [Text] -> Aeson.Value -> ByteString
encodeJson keyOrder =
  Lazy.toStrict . Aeson.encodePretty' (standardConfig keyOrder)
{-# INLINABLE encodeJson #-}

encodeJsonIndented :: [Text] -> Aeson.Value -> ByteString
encodeJsonIndented keyOrder =
  Lazy.toStrict . Aeson.encodePretty' (indentConfig keyOrder)
{-# INLINABLE encodeJsonIndented #-}

decodeJson :: (Aeson.Value -> Aeson.Parser a) -> ByteString -> Either JsonDecodeError a
decodeJson p =
  first (uncurry JsonDecodeError . second Text.pack) .
    Aeson.eitherDecodeStrictWith Aeson.value' (Aeson.iparse p)
{-# INLINABLE decodeJson #-}

standardConfig :: [Text] -> Aeson.Config
standardConfig keyOrder =
  Aeson.defConfig {
      Aeson.confIndent =
        Aeson.Spaces 0
    , Aeson.confCompare =
        Aeson.keyOrder keyOrder
    , Aeson.confNumFormat =
        Aeson.Generic
    }
{-# INLINABLE standardConfig #-}

indentConfig :: [Text] -> Aeson.Config
indentConfig keyOrder =
  Aeson.defConfig {
      Aeson.confIndent =
        Aeson.Spaces 2
    , Aeson.confCompare =
        Aeson.keyOrder keyOrder
    , Aeson.confNumFormat =
        Aeson.Generic
    }
{-# INLINABLE indentConfig #-}

pText :: Aeson.Value -> Aeson.Parser Text
pText =
  Aeson.parseJSON
{-# INLINABLE pText #-}

ppText :: Text -> Aeson.Value
ppText =
  Aeson.toJSON
{-# INLINABLE ppText #-}

pBinary :: Aeson.Value -> Aeson.Parser ByteString
pBinary =
  Aeson.withText "base64 encoded binary data" $ \txt ->
    case Base64.decode $ Text.encodeUtf8 txt of
      Left err ->
        fail $ "could not decode base64 encoded binary data: " <> err
      Right bs ->
        pure bs
{-# INLINABLE pBinary #-}

ppBinary :: ByteString -> Aeson.Value
ppBinary =
  ppText . Text.decodeUtf8 . Base64.encode
{-# INLINABLE ppBinary #-}

pUnit :: Aeson.Value -> Aeson.Parser ()
pUnit =
  Aeson.withObject "object containing unit (i.e. {})" $ \o ->
    if KeyMap.null o then
      pure ()
    else
      fail $
        "expected an object containing unit (i.e. {})," <>
        "\nbut found an object with one or more members"
{-# INLINABLE pUnit #-}

ppUnit :: Aeson.Value
ppUnit =
  Aeson.Object KeyMap.empty
{-# INLINABLE ppUnit #-}

pInt :: Aeson.Value -> Aeson.Parser Int64
pInt =
  Aeson.parseJSON
{-# INLINABLE pInt #-}

ppInt :: Int64 -> Aeson.Value
ppInt =
  Aeson.toJSON
{-# INLINABLE ppInt #-}

pDouble :: Aeson.Value -> Aeson.Parser Double
pDouble =
  Aeson.parseJSON
{-# INLINABLE pDouble #-}

ppDouble :: Double -> Aeson.Value
ppDouble =
  --
  -- This maps NaN/Inf -> 'null', and uses 'fromFloatDigits' to convert
  -- Double -> Scientific.
  --
  -- Don't use 'realToFrac' here, it converts the Double to a Scientific
  -- with far more decimal places than a 64-bit floating point number can
  -- possibly represent.
  --
  Aeson.toJSON
{-# INLINABLE ppDouble #-}

pEnum :: (VariantName -> Maybe (Aeson.Value -> Aeson.Parser a)) -> Aeson.Value -> Aeson.Parser a
pEnum mkParser =
  Aeson.withObject "object containing an enum (i.e. a single member)" $ \o ->
    case KeyMap.toList o of
      [(name, value)] -> do
        case mkParser (VariantName $ toText name) of
          Nothing ->
            fail ("unknown enum variant: " <> toString name) <?> Aeson.Key name
          Just parser ->
            parser value <?> Aeson.Key name
      [] ->
        fail $
          "expected an object containing an enum (i.e. a single member)," <>
          "\nbut found an object with no members"
      kvs ->
        fail $
          "expected an object containing an enum (i.e. a single member)," <>
          "\nbut found an object with more than one member:" <>
          "\n  " <> List.intercalate ", " (fmap (toString . fst) kvs)
{-# INLINABLE pEnum #-}

ppEnum :: Variant Aeson.Value -> Aeson.Value
ppEnum (Variant (VariantName name) value) =
  Aeson.object [
      (fromText name) .= value
    ]
{-# INLINABLE ppEnum #-}

withStructField :: FieldName -> Aeson.Object -> (Aeson.Value -> Aeson.Parser a) -> Aeson.Parser a
withStructField name o p = do
  x <- o .: (fromText . unFieldName $ name)
  p x <?> Aeson.Key (fromText . unFieldName $ name)
{-# INLINABLE withStructField #-}

withOptionalField :: FieldName -> Aeson.Object -> (Aeson.Value -> Aeson.Parser a) -> Aeson.Parser (Maybe a)
withOptionalField name o p = do
  mx <- o .:? (fromText . unFieldName $ name)
  case mx of
    Nothing ->
      pure Nothing
    Just x ->
      Just <$> (p x <?> Aeson.Key (fromText . unFieldName $ name))
{-# INLINABLE withOptionalField #-}

ppStruct :: [Field Aeson.Value] -> Aeson.Value
ppStruct =
  Aeson.object . fmap ppStructField
{-# INLINABLE ppStruct #-}

ppStructField :: Field Aeson.Value -> Aeson.Pair
ppStructField (Field (FieldName name) value) =
  (fromText name) .= value
{-# INLINABLE ppStructField #-}

kmapM :: (Aeson.Value -> Aeson.Parser a) -> Boxed.Vector Aeson.Value -> Aeson.Parser (Boxed.Vector a)
kmapM f =
  Boxed.imapM $ \i x ->
    f x <?> Aeson.Index i
{-# INLINABLE kmapM #-}

pDate :: Aeson.Value -> Aeson.Parser Date
pDate =
  Aeson.withText "date" $
    fromEither renderTimeError .
    parseDate .
    Text.encodeUtf8
{-# INLINABLE pDate #-}

ppDate :: Date -> Aeson.Value
ppDate =
  Aeson.String . Text.decodeUtf8 . renderDate
{-# INLINABLE ppDate #-}

pTime :: Aeson.Value -> Aeson.Parser Time
pTime =
  Aeson.withText "time" $
    fromEither renderTimeError .
    parseTime .
    Text.encodeUtf8
{-# INLINABLE pTime #-}

ppTime :: Time -> Aeson.Value
ppTime =
  Aeson.String . Text.decodeUtf8 . renderTime
{-# INLINABLE ppTime #-}

fromEither :: (x -> Text) -> Either x a -> Aeson.Parser a
fromEither render = \case
  Left err ->
    fail . Text.unpack $ render err
  Right x ->
    pure x
{-# INLINABLE fromEither #-}
