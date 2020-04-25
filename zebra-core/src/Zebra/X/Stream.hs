{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.X.Stream (
    hoistEither
  ) where

import           P
import           Control.Monad.Trans.Class  (lift)
import qualified Control.Monad.Trans.Either as EitherT
import           Streaming.Internal (Stream (..))
import           Viking


hoistEither :: Monad m => (a -> Either e b) -> Stream (Of a) m r -> Stream (Of b) (EitherT.EitherT e m) r
hoistEither f = loop
  where
    loop stream = case stream of
      Return r -> Return r
      Effect m -> Effect (lift (fmap loop m))
      Step (a :> rest) -> Effect $ do
        a0 <- EitherT.hoistEither (f a)
        return $
          Step (a0 :> loop rest)

