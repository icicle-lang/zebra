{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Json.Util where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import           Hedgehog.Gen.QuickCheck (arbitrary)
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Serial.Json.Util


jText :: Gen Text
jText =
  fmap Text.pack . listOf $
    Gen.unicode

jBinary :: Gen ByteString
jBinary =
  fmap ByteString.pack . listOf $
    Gen.integral (Range.linear minBound maxBound)

prop_roundtrip_unit :: Property
prop_roundtrip_unit =
  withTests 1 . property $
    tripping () (\_ -> encodeJson [] ppUnit) (decodeJson pUnit)

prop_roundtrip_int :: Property
prop_roundtrip_int =
  gamble (Gen.integral (Range.linear minBound maxBound)) $ \x ->
    tripping x (encodeJson [] . ppInt) (decodeJson pInt)

prop_roundtrip_date :: Property
prop_roundtrip_date =
  gamble jDate $ \x ->
    tripping x (encodeJson [] . ppDate) (decodeJson pDate)

prop_roundtrip_time :: Property
prop_roundtrip_time =
  gamble jTime $ \x ->
    tripping x (encodeJson [] . ppTime) (decodeJson pTime)

prop_roundtrip_double :: Property
prop_roundtrip_double =
  gamble arbitrary $ \x ->
    tripping x (encodeJson [] . ppDouble) (decodeJson pDouble)

prop_roundtrip_text :: Property
prop_roundtrip_text =
  gamble jText $ \x ->
    tripping x (encodeJson [] . ppText) (decodeJson pText)

prop_roundtrip_binary :: Property
prop_roundtrip_binary =
  gamble jBinary $ \x ->
    tripping x (encodeJson [] . ppBinary) (decodeJson pBinary)

-- This can be considered documentation that for all valid UTF-8 byte
-- sequences, translating them to UTF-16 (i.e. Data.Text / Aeson) and back
-- again results in the original sequence of bytes.
prop_roundtrip_utf8 :: Property
prop_roundtrip_utf8 =
  gamble (Gen.filter (isRight . Text.decodeUtf8') jBinary) $ \x ->
    tripping x Text.decodeUtf8 (Just . Text.encodeUtf8)


tests :: IO Bool
tests =
  checkParallel $$(discover)
