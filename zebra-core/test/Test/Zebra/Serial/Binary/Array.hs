{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Serial.Binary.Array where

import qualified Data.ByteString as ByteString
import qualified Data.List as List
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import           Hedgehog.Gen.QuickCheck (arbitrary)
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.QuickCheck.Instances ()
import           Test.Zebra.Util
import           Test.Zebra.Jack

import           Zebra.Serial.Binary.Array


prop_roundtrip_strings :: Property
prop_roundtrip_strings =
  gamble (Boxed.fromList <$> listOf arbitrary) $ \xs ->
    trippingSerial bStrings (getStrings $ Boxed.length xs) xs

prop_roundtrip_bytes :: Property
prop_roundtrip_bytes =
  gamble arbitrary $ \bs ->
    trippingSerial bByteArray (getByteArray $ ByteString.length bs) bs

prop_roundtrip_sized_bytes :: Property
prop_roundtrip_sized_bytes =
  gamble arbitrary $
    trippingSerial bSizedByteArray getSizedByteArray

prop_roundtrip_ints :: Property
prop_roundtrip_ints =
  gamble (Storable.fromList <$> listOf sizedBounded) $ \xs ->
    trippingSerial bIntArray (getIntArray $ Storable.length xs) xs

-- Worst case for packing is to make sure each pack block contains an int64 min and max.
prop_roundtrip_ints_minmax_64blocks :: Property
prop_roundtrip_ints_minmax_64blocks =
  gamble (Gen.integral (Range.linear 0 1000)) $ \x ->
    let x' = x * 64
        xs = Storable.fromList $ List.take x' $ List.cycle [minBound, maxBound]
    in trippingSerial bIntArray (getIntArray $ Storable.length xs) xs


prop_roundtrip_zigzag :: Property
prop_roundtrip_zigzag =
  gamble sizedBounded $ \x ->
    x === unZigZag64 (zigZag64 x)

prop_mid64 :: Property
prop_mid64 =
  gamble ((,) <$> sizedBounded <*> sizedBounded) $ \(x, y) ->
    fromIntegral (mid64 x y) === midBig (fromIntegral x) (fromIntegral y)

midBig :: Integer -> Integer -> Integer
midBig x y =
  x + (y - x) `div` 2


tests :: IO Bool
tests =
  checkParallel $$(discover)
