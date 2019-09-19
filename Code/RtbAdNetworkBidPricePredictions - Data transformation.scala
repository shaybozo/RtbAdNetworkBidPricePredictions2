// COMMAND ----------

// Read an interval of dates
import java.time._  
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

def lastDaysPartitions(basePath: String, now: LocalDate, noDaysBack: Int, leftPad: Boolean = false): Seq[String] = {
    (1 to noDaysBack).map { days =>
      val pastDay = now.minusDays(days)
      if (leftPad)
        f"$basePath/day=${pastDay.getYear}-${pastDay.getMonthValue}%02d-${pastDay.getDayOfMonth}%02d/hour=*/*"
      else
        f"$basePath/day=${pastDay.getYear}-${pastDay.getMonthValue}%02d-${pastDay.getDayOfMonth}%02d/hour=*/*"
    }
  }

case class RevenueRecord(impressions: Long, revenue: Double) extends Serializable

// COMMAND ----------

// reads the data from S3
val basePath: String = "/mnt/S3/staging-jaquet/AGGREGATION_EVENT_JSON"

val daysToTake: Int = 1

val rawDataDirectories: Seq[String] = lastDaysPartitions(basePath, LocalDate.now(ZoneId.of("UTC")), daysToTake)

val allDataRaw = sqlContext.read.option("basePath", "/mnt/S3/blue-raven/production/mapped").parquet(rawDataDirectories: _*)

allDataRaw.createOrReplaceTempView("allDataRaw")

// COMMAND ----------

// Dates changes
val allDataFliteredWithDates = sqlContext.sql("SELECT *, to_timestamp(eventTimestamp / 1000) AS dateTime FROM allDataRaw WHERE eventType in ('AD_REQUEST') AND sdkEvent = false " )
allDataFliteredWithDates.createOrReplaceTempView("allDataFliteredWithDates")


// COMMAND ----------

// Pre processing before spliting to losing and winning ads bids

// removed all sessions without winning bids ???? (maybe add them - if so remove the winnerBidder) - AND winnerBidderGroup in ('RTB', 'Non-RTB', 'MEDIATION', 'PMN')
// winnerBidderGroup - 'RTB', 'Non-RTB', 'MEDIATION', 'PMN' -> reported only in cases where there is a winning ad
// remove all sdk events (sdkEvent = false)
// remove all events non AD_REQUEST (eventType in ('AD_REQUEST'))
// retrived all relevant data fields 
// encode the follwing fields
// gender StringType => 2 translate it to 0 /1 
// ** converted to male = 1
// 
// adFormat - LongType => 2
// * DISPLAY((byte) 1),
// * VIDEO((byte) 2),
// * PMN((byte) 4),
// 
// auctionType - StringType => 2
// * "First place price"
// * "Second place price"
// * "Fixed price"
// ** converted to zero base
// 
// connectionType - StringType => 2
// * UNKNOWN
// * WIFI
// * 3G
// ** converted to zero base
// 
// inventoryType - StringType => 2
// Video
// Display
// VideoAndDisplay
// None
// ** converted to zero base

// deviceOs
// 'Android' 1 
// ELSE (iOS) 0

// iabLocationType encoded as
// "GPS/Location Services" - 0
// "IP Address" - 1
// "User provided" - 2

val allDataFlitered = sqlContext.sql("SELECT GDPRConsent, CAST(age AS INT) as ageInt, adFormat, dnt as dontTrack, CASE WHEN auctionType = 'First place price' THEN 0 WHEN auctionType = 'Second place price' THEN 1 WHEN auctionType = 'Fixed price' THEN 2 ELSE 3 END AS auctionTypeInt, CASE WHEN connectionType = 'UNKNOWN' THEN 0 WHEN connectionType = 'WIFI' THEN 1 WHEN connectionType = '3G' THEN 2 ELSE 3 END AS connectionTypeInt, CASE WHEN gender = 'M' THEN TRUE ELSE FALSE END as genderBool, CASE WHEN inventoryType = 'Video' THEN 0 WHEN inventoryType = 'Display' THEN 1 WHEN inventoryType = 'VideoAndDisplay' THEN 2 WHEN inventoryType = 'None' THEN 3 ELSE 4 END AS inventoryTypeInt, CASE WHEN city is NULL OR city = '' THEN 'None' ELSE city END AS actualCity, CASE WHEN countryCode is NULL OR countryCode = '' THEN 'None' ELSE countryCode END AS actualCountryCode, clearPrice, floorPrice, CASE WHEN bundleId is NULL OR bundleId = '' THEN 'None' ELSE bundleId END AS actualBundleId, contentId, dealId, CASE WHEN deviceIDType is NULL OR deviceIDType = '' THEN 'None' ELSE deviceIDType END AS actualDeviceIDType, CASE WHEN deviceId is NULL OR deviceId = '' THEN 'None' ELSE deviceId END AS actualDeviceId, CASE WHEN deviceOs = 'Android' THEN 1 ELSE 0 END AS deviceOsInt, CASE WHEN deviceOsVersion is NULL OR deviceOsVersion = '' THEN 'None' ELSE deviceOsVersion END AS actualDeviceOsVersion, CASE WHEN size is NULL OR size = '' THEN 'None' ELSE size END AS actualSize, storeCategories[0] AS storeCategoryRaw, CASE WHEN version is NULL OR version = '' THEN 'None' ELSE version END AS sdkVersion, month(dateTime) AS month, dayofmonth(dateTime) AS dayOfMonth, hour(dateTime) AS hour, minute(dateTime) AS minute, dayofweek(dateTime) as dayOfWeek, CASE WHEN iabLocationType = 'GPS/Location Services' THEN 0 WHEN iabLocationType = 'IP Address' THEN 1 WHEN iabLocationType = 'User provided' THEN 2 ELSE 3 END AS locationTypeInt, CASE WHEN language is NULL OR language = '' THEN 'None' ELSE language END AS actualLanguage, CASE WHEN latitude is NULL OR latitude = '' THEN 'None' ELSE latitude END AS actualLatitude, CASE WHEN longitude is NULL OR longitude = '' THEN 'None' ELSE longitude END AS actualLongitude, CASE WHEN osAndVersion is NULL OR osAndVersion = '' THEN 'None' ELSE osAndVersion END AS actualOsAndVersion, publisherId, CASE WHEN region is NULL OR region = '' THEN 'None' ELSE region END AS actualRegion, CASE WHEN carrier is NULL OR carrier = '' THEN 'None' ELSE carrier END AS actualCarrier, bids, networkInstancePlacementId AS placmentId, extractedCampaignId, creativeId, seatId, adNetworkId, winBid, winnerBidderGroup FROM allDataFliteredWithDates WHERE eventType in ('AD_REQUEST') AND sdkEvent = false " )
allDataFlitered.createOrReplaceTempView("allDataFlitered")

// COMMAND ----------

// Winning bids raw

// remove all mediation winning bids - winnerBidderGroup in ('RTB', 'Non-RTB', 'MEDIATION', 'PMN')
// remove all DISPLAY - 1, VIDEO - 2, PMN - 4, Mediation - 5
// bids column won't be used
// removed all non winning auctions
// actualResponseStatus 'Won' was set as 0
val winningBidsRaw = sqlContext.sql("SELECT GDPRConsent, ageInt, adFormat, dontTrack, auctionTypeInt, connectionTypeInt, genderBool, inventoryTypeInt, actualCity, actualCountryCode, clearPrice, floorPrice, actualBundleId, contentId, dealId, actualDeviceIDType, actualDeviceId, deviceOsInt, actualDeviceOsVersion, actualSize, CASE WHEN storeCategoryRaw is NULL OR storeCategoryRaw = '' THEN 'None' ELSE storeCategoryRaw END AS storeCategory, sdkVersion, month, dayOfMonth, hour, minute, dayofweek, locationTypeInt, actualLanguage, actualLatitude, actualLongitude, actualOsAndVersion, publisherId, actualRegion, actualCarrier, placmentId, CASE WHEN extractedCampaignId is NULL OR extractedCampaignId = '' THEN 'None' ELSE extractedCampaignId END AS actualCampaignId, CASE WHEN creativeId is NULL OR creativeId = '' THEN 'None' ELSE creativeId END AS actualCreativeId, CASE WHEN seatId is NULL OR seatId = '' THEN 'None' ELSE seatId END AS actualSeatId, adNetworkId as actualNetworkId, winBid as actualBidPrice, 0 as actualResponseStatus FROM allDataFlitered WHERE winnerBidderGroup in ('RTB', 'Non-RTB', 'PMN') And adFormat in (1, 2, 4)" )
winningBidsRaw.createOrReplaceTempView("winningBidsRaw")

// COMMAND ----------

// Losing bids raw - explode on bids array that contains all losing bids
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

val losingBidsRawExplode = allDataFlitered.withColumn("bid", explode_outer(col("bids")))
losingBidsRawExplode.createOrReplaceTempView("losingBidsRawExplode")

// COMMAND ----------

// Filter all mediation networks by bid network id
// Filter bid.responseStatus = 'BidNetworkOutOptimizationFilter' || 'DemandQpsThrottling'
// encode bid responseStatus as follows
// 'Won' - 0
// 'BelowFloorPrice' - 1
// 'LostOnPrice' - 2
// 'BlockedOnCampaign' - 3
// 'BlockedOnCreative' - 4
// 'BlockedCategory' - 5
// 'BlockedOnCreativeAttribute' - 6
// 'HTTPError' - 7
// 'Timeout' - 8
// 'NoBid' - 9
// 'Capped' - 10
// 'InvalidResponse' - 11

val losingBidsRaw = sqlContext.sql("SELECT GDPRConsent, ageInt, adFormat, dontTrack, auctionTypeInt, connectionTypeInt, genderBool, inventoryTypeInt, actualCity, actualCountryCode, clearPrice, floorPrice, actualBundleId, contentId, dealId, actualDeviceIDType, actualDeviceId, deviceOsInt, actualDeviceOsVersion, actualSize, CASE WHEN storeCategoryRaw is NULL OR storeCategoryRaw = '' THEN 'None' ELSE storeCategoryRaw END AS storeCategory, sdkVersion, month, dayOfMonth, hour, minute, dayofweek, locationTypeInt, actualLanguage, actualLatitude, actualLongitude, actualOsAndVersion, publisherId, actualRegion, actualCarrier, placmentId, CASE WHEN bid.campaignID is NULL OR bid.campaignID = '' THEN 'None' ELSE bid.campaignID END AS actualCampaignId, CASE WHEN bid.creativeID is NULL OR bid.creativeID = '' THEN 'None' ELSE bid.creativeID END AS actualCreativeId, CASE WHEN bid.seatId is NULL OR bid.seatId = '' THEN 'None' ELSE bid.seatId END AS actualSeatId, bid.networkId as actualNetworkId, bid.price as actualBidPrice, CASE WHEN bid.responseStatus = 'Won' THEN 0 WHEN bid.responseStatus = 'BelowFloorPrice' THEN 1 WHEN bid.responseStatus = 'LostOnPrice' THEN 2 WHEN bid.responseStatus = 'BlockedOnCampaign' THEN 3 WHEN bid.responseStatus = 'BlockedOnCreative' THEN 4 WHEN bid.responseStatus = 'BlockedCategory' THEN 5 WHEN bid.responseStatus = 'BlockedOnCreativeAttribute' THEN 6 WHEN bid.responseStatus = 'HTTPError' THEN 7 WHEN bid.responseStatus = 'Timeout' THEN 8 WHEN bid.responseStatus = 'NoBid' THEN 9 WHEN bid.responseStatus = 'Capped' THEN 10 WHEN bid.responseStatus = 'InvalidResponse' THEN 11 END AS actualResponseStatus  FROM losingBidsRawExplode WHERE bid.networkId not in (628, 629, 630, 631, 632, 633, 634, 635, 689) AND bid.responseStatus not in ('BidNetworkOutOptimization', 'DemandQpsThrottling') ")
losingBidsRaw.createOrReplaceTempView("losingBidsRaw")

// COMMAND ----------

// join the losing and winning bids to a single data frame
val allBidsPreEncoded = sqlContext.sql("SELECT * FROM winningBidsRaw UNION SELECT * FROM losingBidsRaw ")
allBidsPreEncoded.createOrReplaceTempView("allBidsPreEncoded")

// COMMAND ----------

// pipeline which contains a StringIndexer and oneHotEncoder on each values from the categorical columns

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}

val index_ageInt = new StringIndexer()
  .setInputCol("ageInt")
  .setOutputCol("ageInt" + "I")
  .setHandleInvalid("keep")

val index_adFormat = new StringIndexer()
  .setInputCol("adFormat")
  .setOutputCol("adFormat" + "I")
  .setHandleInvalid("keep")

val index_auctionTypeInt = new StringIndexer()
  .setInputCol("auctionTypeInt")
  .setOutputCol("auctionTypeInt" + "I")
  .setHandleInvalid("keep")

val index_connectionTypeInt = new StringIndexer()
  .setInputCol("connectionTypeInt")
  .setOutputCol("connectionTypeInt" + "I")
  .setHandleInvalid("keep")

val index_inventoryTypeInt = new StringIndexer()
  .setInputCol("inventoryTypeInt")
  .setOutputCol("inventoryTypeInt" + "I")
  .setHandleInvalid("keep")

val index_city = new StringIndexer()
  .setInputCol("actualCity")
  .setOutputCol("actualCity" + "I")
  .setHandleInvalid("keep")

val index_countryCode = new StringIndexer()
  .setInputCol("actualCountryCode")
  .setOutputCol("actualCountryCode" + "I")
  .setHandleInvalid("keep")

val index_bundleId = new StringIndexer()
  .setInputCol("actualBundleId")
  .setOutputCol("actualBundleId" + "I")
  .setHandleInvalid("keep")

val index_contentId = new StringIndexer()
  .setInputCol("contentId")
  .setOutputCol("contentId" + "I")
  .setHandleInvalid("keep")

val index_dealId = new StringIndexer()
  .setInputCol("dealId")
  .setOutputCol("dealId" + "I")
  .setHandleInvalid("keep")

val index_deviceIDType = new StringIndexer()
  .setInputCol("actualDeviceIDType")
  .setOutputCol("actualDeviceIDType" + "I")
  .setHandleInvalid("keep")

val index_deviceId = new StringIndexer()
  .setInputCol("actualDeviceId")
  .setOutputCol("actualDeviceId" + "I")
  .setHandleInvalid("keep")

val index_deviceOsInt = new StringIndexer()
  .setInputCol("deviceOsInt")
  .setOutputCol("deviceOsInt" + "I")
  .setHandleInvalid("keep")

val index_deviceOsVersion = new StringIndexer()
  .setInputCol("actualDeviceOsVersion")
  .setOutputCol("actualDeviceOsVersion" + "I")
  .setHandleInvalid("keep")

val index_size = new StringIndexer()
  .setInputCol("actualSize")
  .setOutputCol("actualSize" + "I")
  .setHandleInvalid("keep")
 
val index_storeCategory = new StringIndexer()
  .setInputCol("storeCategory")
  .setOutputCol("storeCategory" + "I")
  .setHandleInvalid("keep")

val index_sdkVersion = new StringIndexer()
  .setInputCol("sdkVersion")
  .setOutputCol("sdkVersion" + "I")
  .setHandleInvalid("keep")

val index_month = new StringIndexer()
  .setInputCol("month")
  .setOutputCol("month" + "I")
  .setHandleInvalid("keep")

val index_dayOfMonth = new StringIndexer()
  .setInputCol("dayOfMonth")
  .setOutputCol("dayOfMonth" + "I")
  .setHandleInvalid("keep")

val index_hour = new StringIndexer()
  .setInputCol("hour")
  .setOutputCol("hour" + "I")
  .setHandleInvalid("keep")

val index_minute = new StringIndexer()
  .setInputCol("minute")
  .setOutputCol("minute" + "I")
  .setHandleInvalid("keep")

val index_dayofweek = new StringIndexer()
  .setInputCol("dayofweek")
  .setOutputCol("dayofweek" + "I")
  .setHandleInvalid("keep")
 
val index_locationTypeInt = new StringIndexer()
  .setInputCol("locationTypeInt")
  .setOutputCol("locationTypeInt" + "I")
  .setHandleInvalid("keep")

val index_language = new StringIndexer()
  .setInputCol("actualLanguage")
  .setOutputCol("actualLanguage" + "I")
  .setHandleInvalid("keep")

val index_latitude = new StringIndexer()
  .setInputCol("actualLatitude")
  .setOutputCol("actualLatitude" + "I")
  .setHandleInvalid("keep")

val index_longitude = new StringIndexer()
  .setInputCol("actualLongitude")
  .setOutputCol("actualLongitude" + "I")
  .setHandleInvalid("keep")

val index_osAndVersion = new StringIndexer()
  .setInputCol("actualOsAndVersion")
  .setOutputCol("actualOsAndVersion" + "I")
  .setHandleInvalid("keep")
 
val index_publisherId = new StringIndexer()
  .setInputCol("publisherId")
  .setOutputCol("publisherId" + "I")
  .setHandleInvalid("keep")

val index_region = new StringIndexer()
  .setInputCol("actualRegion")
  .setOutputCol("actualRegion" + "I")
  .setHandleInvalid("keep")

val index_carrier = new StringIndexer()
  .setInputCol("actualCarrier")
  .setOutputCol("actualCarrier" + "I")
  .setHandleInvalid("keep")

val index_placmentId = new StringIndexer()
  .setInputCol("placmentId")
  .setOutputCol("placmentId" + "I")
  .setHandleInvalid("keep")

val index_actualCampaignId = new StringIndexer()
  .setInputCol("actualCampaignId")
  .setOutputCol("actualCampaignId" + "I")
  .setHandleInvalid("keep")

val index_actualCreativeId = new StringIndexer()
  .setInputCol("actualCreativeId")
  .setOutputCol("actualCreativeId" + "I")
  .setHandleInvalid("keep")

val index_actualSeatId = new StringIndexer()
  .setInputCol("actualSeatId")
  .setOutputCol("actualSeatId" + "I")
  .setHandleInvalid("keep")

val index_actualNetworkId = new StringIndexer()
  .setInputCol("actualNetworkId")
  .setOutputCol("actualNetworkId" + "I")
  .setHandleInvalid("keep")

val oneHotEncoder = new OneHotEncoderEstimator()
  .setInputCols(Array(
"ageIntI",
"adFormatI",
"auctionTypeIntI",
"connectionTypeIntI",
"inventoryTypeIntI",
"actualCityI",
"actualCountryCodeI",
"actualBundleIdI",
"contentIdI",
"dealIdI",
"actualDeviceIDTypeI",
"actualDeviceIdI",
"deviceOsIntI",
"actualDeviceOsVersionI",
"actualSizeI",
"storeCategoryI",
"sdkVersionI",
"monthI",
"dayOfMonthI",
"hourI",
"minuteI",
"dayofweekI",
"locationTypeIntI",
"actualLanguageI",
"actualLatitudeI",
"actualLongitudeI",
"actualOsAndVersionI",
"publisherIdI",
"actualRegionI",
"actualCarrierI",
"placmentIdI",
"actualCampaignIdI",
"actualCreativeIdI",
"actualSeatIdI",
"actualNetworkIdI"
))
.setOutputCols(Array(
"ageInt_oneHotEncoded",
"adFormat_oneHotEncoded",
"auctionTypeInt_oneHotEncoded",
"connectionTypeInt_oneHotEncoded",
"inventoryTypeInt_oneHotEncoded",
"city_oneHotEncoded",
"countryCode_oneHotEncoded",
"bundleId_oneHotEncoded",
"contentId_oneHotEncoded",
"dealId_oneHotEncoded",
"deviceIDType_oneHotEncoded",
"deviceId_oneHotEncoded",
"deviceOsInt_oneHotEncoded",
"deviceOsVersion_oneHotEncoded",
"size_oneHotEncoded",
"storeCategory_oneHotEncoded",
"sdkVersion_oneHotEncoded",
"month_oneHotEncoded",
"dayOfMonth_oneHotEncoded",
"hour_oneHotEncoded",
"minute_oneHotEncoded",
"dayofweek_oneHotEncoded",
"locationTypeInt_oneHotEncoded",
"language_oneHotEncoded",
"latitude_oneHotEncoded",
"longitude_oneHotEncoded",
"osAndVersion_oneHotEncoded",
"publisherId_oneHotEncoded",
"region_oneHotEncoded",
"carrier_oneHotEncoded",
"placmentId_oneHotEncoded",
"actualCampaignId_oneHotEncoded",
"actualCreativeId_oneHotEncoded",
"actualSeatId_oneHotEncoded",
"actualNetworkId_oneHotEncoded"
))
.setHandleInvalid("keep")

// still need encoding and Y labeling
//StructField(actualResponseStatus,IntegerType,true)
//StructField(clearPrice,DoubleType,true)
//StructField(floorPrice,DoubleType,true)
//StructField(actualBidPrice,DoubleType,true)

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}

val pipeline = new Pipeline().setStages(Array(
index_ageInt,
index_adFormat,
index_auctionTypeInt,
index_connectionTypeInt,
index_inventoryTypeInt,
index_city,
index_countryCode,
index_bundleId,
index_contentId,
index_dealId,
index_deviceIDType,
index_deviceId,
index_deviceOsInt,
index_deviceOsVersion,
index_size,
index_storeCategory,
index_sdkVersion,
index_month,
index_dayOfMonth,
index_hour,
index_minute,
index_dayofweek,
index_locationTypeInt,
index_language,
index_latitude,
index_longitude,
index_osAndVersion,
index_publisherId,
index_region,
index_carrier,
index_placmentId,
index_actualCampaignId,
index_actualCreativeId,
index_actualSeatId,
index_actualNetworkId,
oneHotEncoder
))

val allBidsEncoded = pipeline.fit(allBidsPreEncoded).transform(allBidsPreEncoded)
allBidsEncoded.createOrReplaceTempView("allBidsEncoded")

// COMMAND ----------

//dropping the redundent columns
val allBidsEncodedFinal = allBidsEncoded.drop(
"ageInt",
"adFormat",
"auctionTypeInt",
"connectionTypeInt",
"inventoryTypeInt",
"actualCity",
"actualCountryCode",
"actualBundleId",
"contentId",
"dealId",
"actualDeviceIDType",
"actualDeviceId",
"deviceOsInt",
"actualDeviceOsVersion",
"actualSize",
"storeCategory",
"sdkVersion",
"month",
"dayOfMonth",
"hour",
"minute",
"dayofweek",
"locationTypeInt",
"actualLanguage",
"actualLatitude",
"actualLongitude",
"actualOsAndVersion",
"publisherId",
"actualRegion",
"actualCarrier",
"placmentId",
"actualCampaignId",
"actualCreativeId",
"actualSeatId",
"actualNetworkId",
"ageIntI",
"adFormatI",
"auctionTypeIntI",
"connectionTypeIntI",
"inventoryTypeIntI",
"actualCityI",
"actualCountryCodeI",
"actualBundleIdI",
"contentIdI",
"dealIdI",
"actualDeviceIDTypeI",
"actualDeviceIdI",
"deviceOsIntI",
"actualDeviceOsVersionI",
"actualSizeI",
"storeCategoryI",
"sdkVersionI",
"monthI",
"dayOfMonthI",
"hourI",
"minuteI",
"dayofweekI",
"locationTypeIntI",
"actualLanguageI",
"actualLatitudeI",
"actualLongitudeI",
"actualOsAndVersionI",
"publisherIdI",
"actualRegionI",
"actualCarrierI",
"placmentIdI",
"actualCampaignIdI",
"actualCreativeIdI",
"actualSeatIdI",
"actualNetworkIdI"
)
allBidsEncodedFinal.createOrReplaceTempView("allBidsEncodedFinal")

// COMMAND ----------

allBidsEncodedFinal.schema

// COMMAND ----------

// break the data randomlly to train and test
val train_test_groups = allBidsEncodedFinal.randomSplit(Array[Double](0.7, 0.3), 18)

// COMMAND ----------

//allBidsEncoded.write.parquet("/mnt/S3/staging-rtbAdNetworkBidPricePredictions_Data_transformation/")

// COMMAND ----------

import org.apache.spark.sql.functions.rand

train_test_groups(0)
.orderBy(rand())
.coalesce(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("sep", ",")
.option("header", "true")
.save("/mnt/S3/ia-staging-test/MlTrain")

// COMMAND ----------

import org.apache.spark.sql.functions.rand

train_test_groups(1)
.orderBy(rand())
.coalesce(1)
.write
.mode(SaveMode.Overwrite)
.format("csv")
.option("sep", ",")
.option("header", "true")
.save("/mnt/S3/ia-staging-test/MlTest")
