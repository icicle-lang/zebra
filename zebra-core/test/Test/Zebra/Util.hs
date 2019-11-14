{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Test.Zebra.Util (
    liftE
  , trippingIO
  , trippingByIO
  , trippingSerial
  , trippingSerialE
  , runGetEither
  , runGetEitherConsumeAll
  ) where

import           Control.Monad.Trans.Class (lift)
import           Control.Monad.IO.Class (MonadIO (..))

import           Data.Binary.Get (Get, ByteOffset)
import qualified Data.Binary.Get as Get
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as Lazy
import           Data.String (String)
import           Data.Void (Void)

import           Hedgehog (PropertyT, MonadTest, annotate)
import           Hedgehog.Internal.Property (failDiff)

import           Text.Show.Pretty (ppShow)
import qualified Text.Show.Pretty as Pretty

import           P

import           System.IO (IO)

import           Control.Monad.Trans.Either (EitherT, runEitherT)


data TrippingError x y =
    EncodeError x
  | DecodeError y
    deriving (Eq, Show)

liftE :: IO a -> EitherT Void IO a
liftE =
  lift

trippingIO ::
     Eq a
  => Eq x
  => Eq y
  => Show a
  => Show x
  => Show y
  => MonadTest m
  => MonadIO m
  => (a -> EitherT x IO b)
  -> (b -> EitherT y IO a)
  -> a
  -> m ()
trippingIO =
  trippingByIO id

trippingByIO ::
     Eq c
  => Eq x
  => Eq y
  => Show c
  => Show x
  => Show y
  => MonadTest m
  => MonadIO m
  => (a -> c)
  -> (a -> EitherT x IO b)
  -> (b -> EitherT y IO a)
  -> a
  -> m ()
trippingByIO select to from a = do
  roundtrip <-
    liftIO . runEitherT $ do
      b <- firstT EncodeError $ to a
      firstT DecodeError $ from b

  diff (pure $ select a) (fmap select roundtrip)


trippingSerial :: forall a. (Eq a, Show a) => (a -> Builder) -> Get a -> a -> PropertyT IO ()
trippingSerial build0 get a =
  let
    build :: a -> Either () Builder
    build =
      pure . build0
  in
    trippingSerialE build get a

trippingSerialE :: (Eq a, Show a, Show x) => (a -> Either x Builder) -> Get a -> a -> PropertyT IO ()
trippingSerialE build get a =
  let
    roundtrip = do
      b <- bimap ppShow Builder.toLazyByteString $ build a
      first ppShow $ runGetEither get b
  in
    diff (pure a) roundtrip

diff :: (Eq x, Eq a, Show x, Show a, MonadTest m) => Either x a -> Either x a -> m ()
diff original roundtrip = do
  let
    comparison =
      "=== Original ===" <>
      "\n" <> ppShow original <>
      "\n" <>
      "\n=== Roundtrip ===" <>
      "\n" <> ppShow roundtrip

    pdiff = do
      o <- Pretty.reify original
      r <- Pretty.reify roundtrip

      pure $ do
        annotate $ "=== - Original / + Roundtrip ==="
        failDiff o r

  unless (roundtrip == original) $ do
    annotate ""
    annotate "Roundtrip failed."
    annotate ""
    fromMaybe (annotate comparison) pdiff


runGetEither :: Get a -> Lazy.ByteString -> Either (Lazy.ByteString, ByteOffset, String) a
runGetEither g =
  let
    third (_, _, x) = x
  in
    second third . Get.runGetOrFail g

runGetEitherConsumeAll :: Get a -> Lazy.ByteString -> Either (Lazy.ByteString, ByteOffset, String) a
runGetEitherConsumeAll g bs =
  case Get.runGetOrFail g bs of
    Left err -> Left err
    Right (leftovers,off,v)
     | Lazy.null leftovers
     -> Right v
     | otherwise
     -> Left (leftovers, off, "Not all input consumed")
