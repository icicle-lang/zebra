{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
module Test.Zebra.Serial.Binary.File where

import           Control.Monad.Trans.Class (lift)

import qualified Data.Binary.Get as Get
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import           Hedgehog.Gen.QuickCheck
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Test.QuickCheck.Instances ()
import           Test.Zebra.Util
import           Test.Zebra.Jack

import           Control.Monad.Trans.Either
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Stream as Stream

import           Zebra.Serial.Binary.Array
import           Zebra.Serial.Binary.File


jSplits :: Gen (Boxed.Vector B.ByteString)
jSplits = arbitrary

checkDecodeGetAll :: (MonadTest m, Show a, Eq a) => Get.Get a -> Stream.Stream Stream.Id B.ByteString -> m ()
checkDecodeGetAll get bss = do
  let
    bs = fold $ Stream.listOfStream bss
    one = runGetEitherConsumeAll (many get) (BL.fromStrict bs)
    alls = Stream.unId $ runEitherT $ Stream.listOfStreamM $ decodeGetAll get $ Stream.trans lift bss

  annotate (show alls)
  first (const ()) one === first (const ()) alls

checkDecodeGetOne :: (MonadTest m, Show a, Eq a) => Get.Get a -> Stream.Stream Stream.Id B.ByteString -> m ()
checkDecodeGetOne get bss = do
  let
    bs = fold $ Stream.listOfStream bss
    expect = getExpect $ Get.runGetOrFail get (BL.fromStrict bs)
    actual = getActual $ Stream.unId $ runEitherT $ decodeGetOne get $ Stream.trans lift bss

  annotate (show actual)
  first (const ()) expect === first (const ()) actual

getExpect ::
     Either x (BL.ByteString, o, a)
  -> Either x (B.ByteString, a)
getExpect m = do
  (leftovers, _, val) <- m
  pure (BL.toStrict leftovers, val)

getActual ::
     Either FileError (a, Stream.Stream (EitherT FileError Stream.Id) B.ByteString)
  -> Either FileError (B.ByteString, a)
getActual m = do
  (val, leftovers) <- m
  fmap ((, val) . fold) . Stream.unId . runEitherT $
    Stream.listOfStreamM leftovers

prop_runStreamOne_getWord64be :: Property
prop_runStreamOne_getWord64be =
  gamble jSplits $ \bss ->
    checkDecodeGetOne Get.getWord64be (Stream.streamOfVector bss)

prop_runStreamOne_getByteArray :: Property
prop_runStreamOne_getByteArray =
  gamble jSplits $ \bss ->
    checkDecodeGetOne getSizedByteArray (Stream.streamOfVector bss)

-- Adding a filter makes sure there are "Skips" in the stream
prop_runStreamOne_getByteArray_filtered :: Property
prop_runStreamOne_getByteArray_filtered =
  gamble jSplits $ \bss ->
    checkDecodeGetOne getSizedByteArray (Stream.filter (\b -> B.length b `mod` 2 == 0) $ Stream.streamOfVector bss)


prop_runStreamOne_getIntArray_prefix_size :: Property
prop_runStreamOne_getIntArray_prefix_size =
  gamble jSplits $ \bss ->
    checkDecodeGetOne get (Stream.streamOfVector bss)
  where
   get = do
    i <- Get.getWord32be
    getIntArray (fromIntegral i)

prop_runStreamOne_getIntArray_static_size :: Property
prop_runStreamOne_getIntArray_static_size =  property $ do
  bss <- forAll jSplits
  num <- forAll arbitrary

  checkDecodeGetOne (getIntArray $ abs num) (Stream.streamOfVector bss)

-- Adding a filter makes sure there are "Skips" in the stream
prop_runStreamOne_getStrings_filtered :: Property
prop_runStreamOne_getStrings_filtered =  property $ do
  bss <- forAll jSplits
  num <- forAll (Gen.integral (Range.linear 0 100))

  checkDecodeGetOne (getStrings num) (Stream.filter (\b -> B.length b `mod` 2 == 0) $ Stream.streamOfVector bss)

prop_runStreamMany_getWord64be :: Property
prop_runStreamMany_getWord64be =
  gamble jSplits $ \bss ->
    checkDecodeGetAll Get.getWord64be (Stream.streamOfVector bss)

prop_runStreamMany_getByteArray :: Property
prop_runStreamMany_getByteArray =
  gamble jSplits $ \bss ->
    checkDecodeGetAll getSizedByteArray (Stream.streamOfVector bss)

-- Adding a filter makes sure there are "Skips" in the stream
prop_runStreamMany_getByteArray_filtered :: Property
prop_runStreamMany_getByteArray_filtered =
  gamble jSplits $ \bss ->
    checkDecodeGetAll getSizedByteArray (Stream.filter (\b -> B.length b `mod` 2 == 0) $ Stream.streamOfVector bss)


prop_runStreamMany_getIntArray_prefix_size :: Property
prop_runStreamMany_getIntArray_prefix_size =
  gamble jSplits $ \bss ->
    checkDecodeGetAll get (Stream.streamOfVector bss)
  where
   get = do
    i <- Get.getWord32be
    getIntArray (fromIntegral i)

prop_runStreamMany_getIntArray_static_size :: Property
prop_runStreamMany_getIntArray_static_size = property $ do
  bss <- forAll jSplits
  num <- forAll arbitrary

  checkDecodeGetAll (getIntArray $ abs num) (Stream.streamOfVector bss)

-- Adding a filter makes sure there are "Skips" in the stream
prop_runStreamMany_getStrings_filtered :: Property
prop_runStreamMany_getStrings_filtered = property $ do
  bss <- forAll jSplits
  num <- forAll (Gen.integral (Range.linear 0 100))

  checkDecodeGetAll (getStrings num) (Stream.filter (\b -> B.length b `mod` 2 == 0) $ Stream.streamOfVector bss)


tests :: IO Bool
tests =
  checkParallel $$(discover)
