{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.X.Either (
    firstJoin
  , secondJoin
  , tryEitherT
  ) where

import           P
import           Control.Exception.Base (Exception)
import           Control.Monad.Catch (MonadCatch, try)
import           Control.Monad.Trans.Either (EitherT, runEitherT, newEitherT, firstEitherT)

firstJoin :: (Functor m, Monad m) => (x -> y) -> EitherT x (EitherT y m) a -> EitherT y m a
firstJoin f e =
  newEitherT $ do
    b <- runEitherT (runEitherT e)
    case b of
      Left a ->
        pure (Left a)
      Right x ->
        pure (first f x)
{-# INLINE firstJoin #-}

secondJoin :: (Functor m, Monad m) => (y -> x) -> EitherT x (EitherT y m) a -> EitherT x m a
secondJoin f e =
  newEitherT $ do
    b <- runEitherT (runEitherT e)
    case b of
      Left a ->
        pure (Left (f a))
      Right x ->
        pure x
{-# INLINE secondJoin #-}

tryEitherT :: (Functor m, MonadCatch m, Exception e) => (e -> x) -> m a -> EitherT x m a
tryEitherT handler = firstEitherT handler . newEitherT . try
{-# INLINE tryEitherT #-}