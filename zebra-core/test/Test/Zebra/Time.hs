{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Zebra.Time where

import qualified Data.ByteString.Char8 as Char8
import qualified Data.Time as Time

import           Hedgehog

import           P

import           System.IO (IO)

import           Test.Zebra.Jack

import           Zebra.Time


prop_roundtrip_date_render :: Property
prop_roundtrip_date_render =
  gamble jDate $ \x ->
    tripping x renderDate parseDate

prop_roundtrip_date_days :: Property
prop_roundtrip_date_days =
  gamble jDate $ \x ->
    tripping x toDays fromDays

prop_roundtrip_date_calendar :: Property
prop_roundtrip_date_calendar =
  gamble jDate $ \x ->
    tripping x toCalendarDate fromCalendarDate

prop_roundtrip_time_render :: Property
prop_roundtrip_time_render =
  gamble jTime $ \x ->
    tripping x renderTime parseTime

prop_roundtrip_time_seconds :: Property
prop_roundtrip_time_seconds =
  gamble jTime $ \time0 ->
  let
    Right time =
      fromSeconds (toSeconds time0)
  in
    tripping time toSeconds fromSeconds

prop_roundtrip_time_milliseconds :: Property
prop_roundtrip_time_milliseconds =
  gamble jTime $ \time0 ->
  let
    Right time =
      fromMilliseconds (toMilliseconds time0)
  in
    tripping time toMilliseconds fromMilliseconds

prop_roundtrip_time_microseconds :: Property
prop_roundtrip_time_microseconds =
  gamble jTime $ \x ->
    tripping x toMicroseconds fromMicroseconds

prop_roundtrip_time_calendar :: Property
prop_roundtrip_time_calendar =
  gamble jTime $ \x ->
    tripping x toCalendarTime fromCalendarTime

prop_roundtrip_time_of_day_microsecond :: Property
prop_roundtrip_time_of_day_microsecond =
  gamble jTimeOfDay $ \x ->
    tripping x fromTimeOfDay (Just . toTimeOfDay)

epoch :: Time.UTCTime
epoch =
  Time.UTCTime (Time.fromGregorian 1600 3 1) 0

prop_compare_date_parsing :: Property
prop_compare_date_parsing =
  gamble jDate $ \date -> do
    let
      str =
        renderDate date

      theirs :: Maybe Time.Day
      theirs =
        Time.parseTimeM False Time.defaultTimeLocale "%Y-%m-%d" $
        Char8.unpack str

      theirs_days :: Maybe Days
      theirs_days =
        fmap (fromIntegral . Time.toModifiedJulianDay) theirs

      ours :: Maybe Days
      ours =
        fmap toModifiedJulianDay .
        rightToMaybe $
        parseDate str

    annotate ("render =  " <> Char8.unpack str)
    annotate ("theirs =  " <> show theirs)
    annotate ("ours   =  " <> show ours)
    theirs_days === ours

prop_compare_time_parsing :: Property
prop_compare_time_parsing =
  gamble jTime $ \time -> do
    let
      str =
        renderTime time

      theirs :: Maybe Time.UTCTime
      theirs =
        Time.parseTimeM False Time.defaultTimeLocale "%Y-%m-%d %H:%M:%S%Q" $
        Char8.unpack str

      theirs_us :: Maybe Microseconds
      theirs_us =
        fmap round .
        fmap (* 1000000) $
        fmap (`Time.diffUTCTime` epoch) theirs

      ours :: Maybe Microseconds
      ours =
        fmap toMicroseconds .
        rightToMaybe $
        parseTime str

    annotate ("render =  " <> Char8.unpack str)
    annotate ("theirs =  " <> show theirs)
    annotate ("ours   =  " <> show ours)
    theirs_us === ours


tests :: IO Bool
tests =
  checkParallel $$(discover)
