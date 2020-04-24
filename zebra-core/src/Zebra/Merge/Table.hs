{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Zebra.Merge.Table (
    MaximumRowSize(..)
  , MergeRowsPerBlock(..)

  , UnionTableError(..)
  , renderUnionTableError

  , unionStriped
  , unionStripedWith
  ) where

import           Control.Monad.Morph (hoist, squash)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Either (EitherT, newEitherT, runEitherT, hoistEither, left)

import qualified Data.Map.Strict as Map
import qualified Data.Vector as Boxed

import           P

import           Streaming.Internal (Stream (..))
import           Viking (Of (..))
import qualified Viking.Stream as Stream

import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector.Cons as Cons

import           Zebra.Table.Logical (LogicalSchemaError, LogicalMergeError)
import qualified Zebra.Table.Logical as Logical
import           Zebra.Table.Schema (SchemaUnionError)
import qualified Zebra.Table.Schema as Schema
import           Zebra.Table.Striped (StripedError)
import qualified Zebra.Table.Striped as Striped


newtype MaximumRowSize =
  MaximumRowSize {
      unMaximumRowSize :: Int64
    } deriving (Eq, Ord, Show)

newtype MergeRowsPerBlock =
  MergeRowsPerBlock {
      unMergeRowsPerBlock :: Int
    } deriving (Eq, Ord, Show)

data Row =
  Row {
      rowKey :: !Logical.Value
    , rowValue :: !Logical.Value
  } deriving (Eq, Ord, Show)

data UnionTableError =
    UnionEmptyInput
  | UnionStripedError !StripedError
  | UnionLogicalSchemaError !LogicalSchemaError
  | UnionLogicalMergeError !LogicalMergeError
  | UnionSchemaError !SchemaUnionError
  | UnionNotMap
    deriving (Eq, Show)

renderUnionTableError :: UnionTableError -> Text
renderUnionTableError = \case
  UnionEmptyInput ->
    "Cannot merge empty files"
  UnionStripedError err ->
    Striped.renderStripedError err
  UnionLogicalSchemaError err ->
    Logical.renderLogicalSchemaError err
  UnionLogicalMergeError err ->
    Logical.renderLogicalMergeError err
  UnionSchemaError err ->
    Schema.renderSchemaUnionError err
  UnionNotMap ->
    "Can not merge zebra files which aren't maps"

------------------------------------------------------------------------
-- General

unionSchemas :: Cons Boxed.Vector Schema.Table -> Either UnionTableError Schema.Table
unionSchemas =
  first UnionSchemaError . Cons.fold1M' Schema.union
{-# INLINABLE unionSchemas #-}

peekHead :: Monad m => Stream (Of x) m r -> EitherT UnionTableError m (x, Stream (Of x) m r)
peekHead input = do
  e <- lift $ Stream.next input
  case e of
    Left _r ->
      left UnionEmptyInput
    Right (hd, tl) ->
      pure (hd, Stream.cons hd tl)
{-# INLINABLE peekHead #-}

streamStripedAsRows ::
     Monad m
  => Int
  -> Stream (Of Striped.Table) m ()
  -> Stream (Of Row) (EitherT UnionTableError m) ()
streamStripedAsRows _num stream =
  Stream.map (uncurry Row) $
    Stream.concat $
    Stream.mapM (hoistEither . logicalPairs) $
    hoist lift
      stream
{-# INLINABLE streamStripedAsRows #-}

mergeStreams ::
     Monad m
  => Stream (Of Row) (EitherT UnionTableError m) ()
  -> Stream (Of Row) (EitherT UnionTableError m) ()
  -> Stream (Of Row) (EitherT UnionTableError m) ()
mergeStreams leftStream rightStream = Effect . newEitherT $ do
  left0  <- runEitherT $ Stream.next leftStream
  right0 <- runEitherT $ Stream.next rightStream

  return $ do
    left1 <- left0
    right1 <- right0
    case (left1, right1) of
      (Left (), Left ()) ->
        return (Return ())

      (Left (), Right (right2, rightRest)) ->
        return $
          Step (right2 :> rightRest)

      (Right (left2, leftRest), Left ()) ->
        return $
          Step (left2 :> leftRest)

      (Right (left2@(Row leftKey leftValue), leftRest), Right (right2@(Row rightKey rightValue), rightRest)) ->
        case compare leftKey rightKey of
          LT ->
            return $
              Step (left2 :> mergeStreams leftRest (Step (right2 :> rightRest)))
          EQ -> do
            merged <-
              first UnionLogicalMergeError $
                Row leftKey <$> Logical.mergeValue leftValue rightValue
            return $
              Step (merged :> mergeStreams leftRest rightRest)
          GT ->
            return $
              Step (right2 :> mergeStreams (Step (left2 :> leftRest)) rightRest)

-- merge streams in a binary tree fashion
mergeStreamsBinary ::
     Monad m
  => Cons Boxed.Vector (Stream (Of Row) (EitherT UnionTableError m) ())
  -> Stream (Of Row) (EitherT UnionTableError m) ()
mergeStreamsBinary kvss =
  case Cons.length kvss of
    1 ->
      Cons.head kvss

    2 -> do
      let k = Cons.toVector kvss
      mergeStreams
        (k Boxed.! 0)
        (k Boxed.! 1)

    n -> do
      let
        (kvss0, kvss1) = Boxed.splitAt (n `div` 2) $ Cons.toVector kvss
        kvs0 = mergeStreamsBinary $ Cons.unsafeFromVector kvss0
        kvs1 = mergeStreamsBinary $ Cons.unsafeFromVector kvss1
      mergeStreamsBinary $ Cons.from2 kvs0 kvs1
{-# INLINABLE mergeStreamsBinary #-}


unionStripedWith ::
     Monad m
  => Schema.Table
  -> Maybe MaximumRowSize
  -> MergeRowsPerBlock
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStripedWith schema _msize blockRows inputs0 = do
  let
    fromStriped =
      Stream.mapM (hoistEither . first UnionStripedError . Striped.transmute schema) .
      hoist lift

  hoist squash $
    Stream.mapM (hoistEither . first UnionStripedError . Striped.fromLogical schema) $
    Stream.whenEmpty (Logical.empty schema) $
    chunkRows blockRows $
    mergeStreamsBinary $
    Cons.imap streamStripedAsRows $
      (fmap fromStriped inputs0)
{-# INLINABLE unionStripedWith #-}

unionStriped ::
     Monad m
  => Maybe MaximumRowSize
  -> MergeRowsPerBlock
  -> Cons Boxed.Vector (Stream (Of Striped.Table) m ())
  -> Stream (Of Striped.Table) (EitherT UnionTableError m) ()
unionStriped msize blockRows inputs0 = do
  (heads, inputs1) <- fmap Cons.unzip . lift $ traverse peekHead inputs0
  schema           <- lift . hoistEither . unionSchemas $ fmap Striped.schema heads
  unionStripedWith schema msize blockRows inputs1
{-# INLINABLE unionStriped #-}

-- | Groups together the rows as per Chunk Size and forms a logical table from them
chunkRows ::
     Monad m
  => MergeRowsPerBlock
  -> Stream (Of Row) (EitherT UnionTableError m) ()
  -> Stream (Of Logical.Table) (EitherT UnionTableError m) ()
chunkRows blockRows inputs =
  let
    rowsToTable =
      Logical.Map . Map.fromDistinctAscList . fmap (\(Row k v) -> (k, v))
  in
    Stream.map rowsToTable $
      Stream.mapped Stream.toList $
      Stream.chunksOf (unMergeRowsPerBlock blockRows)
        inputs
{-# INLINABLE chunkRows #-}

-- | Convert striped table to a vector of logical key, value pairs
--   tries to be strict on the key, lazy on the value
logicalPairs ::
     Striped.Table
  -> Either UnionTableError (Boxed.Vector (Logical.Value, Logical.Value))
logicalPairs (Striped.Map _ k v) = do
  !ks <- first UnionStripedError $ Striped.toValues k
  vs  <- first UnionStripedError $ Striped.toValues v
  pure $ Boxed.zip ks vs
logicalPairs _ =
  Left UnionNotMap
{-# INLINABLE logicalPairs #-}
