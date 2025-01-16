package com.amazonaws.emr.api

import com.amazonaws.emr.api.AwsPricing.ArchitectureType.ArchitectureType
import com.amazonaws.emr.api.AwsPricing.ComputeType.ComputeType
import com.amazonaws.emr.api.AwsPricing.VolumeType.VolumeType
import org.json4s._
import org.json4s.jackson.JsonMethods._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.pricing.PricingClient
import software.amazon.awssdk.services.pricing.model.{Filter, GetProductsRequest}

import scala.collection.JavaConverters._

/**
 * Utility class to retrieve AWS services pricing
 */
object AwsPricing {

  val DefaultCurrency = "USD"

  private val EmrServiceCode = "ElasticMapReduce"
  private val EmrProductFamily = "Elastic Map Reduce Instance"
  private val EmrSoftwareType = "EMR"

  private val EmrContainersFamily = "EMR Containers"
  private val EmrContainersOperation = "StartJobRun"

  private val Ec2ServiceCode = "AmazonEc2"

  private case class EbsSpec(number: Int, sizeGiB: Int, totalSizeGiB: Int)

  private val EmrEbsDefaults = Map(
    "large" -> EbsSpec(1, 32, 32),
    "xlarge" -> EbsSpec(2, 32, 64),
    "2xlarge" -> EbsSpec(4, 32, 128),
    "4xlarge" -> EbsSpec(4, 64, 256),
    "8xlarge" -> EbsSpec(4, 128, 512),
    "9xlarge" -> EbsSpec(4, 144, 576),
    "10xlarge" -> EbsSpec(4, 160, 640),
    "12xlarge" -> EbsSpec(4, 192, 768),
    "16xlarge" -> EbsSpec(4, 256, 1024),
    "18xlarge" -> EbsSpec(4, 288, 1152),
    "24xlarge" -> EbsSpec(4, 384, 1536),
    "default" -> EbsSpec(4, 128, 512)
  )

  case class EmrServerlessPrice(
    arch: ArchitectureType,
    CPUHoursPrice: Double,
    GBHoursPrice: Double,
    storagePrice: Double
  )

  case class EmrContainersPrice(
    compute: ComputeType,
    GBHoursPrice: Double,
    CPUHoursPrice: Double
  )

  case class EmrInstance(
    instanceType: String,
    instanceFamily: String,
    currentGeneration: Boolean,
    vCpu: Int,
    memoryGiB: Int,
    volumeType: VolumeType,
    volumeNumber: Int,
    volumeSizeGB: Int,
    networkBandwidthGbps: Int,
    yarnMaxMemoryMB: Int,
    ec2Price: Double,
    emrPrice: Double
  )

  private case class EmrInstancePrice(
    instanceType: String,
    instanceFamily: String,
    price: Double
  )

  private case class Ec2InstancePrice(
    instanceType: String,
    instanceFamily: String,
    memoryGiB: Int,
    vCpu: Int,
    processorArchitecture: String,
    price: Double,
    currentGeneration: Boolean,
    volumeType: VolumeType,
    volumeNumber: Int,
    volumeSizeGB: Int,
    networkBandwidthGbps: Int
  )

  case class EbsPrice(
    volumeType: String,
    price: Double
  )

  object ArchitectureType extends Enumeration {
    type ArchitectureType = Value
    val X86_64, ARM64 = Value
  }

  object ComputeType extends Enumeration {
    type ComputeType = Value
    val EC2, FARGATE = Value
  }

  object VolumeType extends Enumeration {
    type VolumeType = Value
    val EBS, NVME, SSD, HDD = Value
  }

  // Pricing API only available in US_EAST_1
  private val client = PricingClient.builder().region(Region.US_EAST_1).build()

  implicit val formats: Formats = DefaultFormats

  /**
   * Retrieve details on EMR Service Costs for all instances in the specified Region.
   * Note: the guy who invented the schema for AWS pricing is a fucking idiot and should burn in hell.
   *
   * @param region AWS Region that will be used. If none is specified, default to us-east-1 (N. Virginia)
   * @return List[EmrInstancePrice]
   */
  private def getEmrServicePrice(region: String = Region.US_EAST_1.toString): List[EmrInstancePrice] = {

    case class EmrService(product: EmrProduct, serviceCode: String, terms: JObject, publicationDate: String)
    case class EmrProduct(productFamily: String, attributes: EmrAttributes)
    case class EmrAttributes(regionCode: String, servicecode: String, instanceType: String, instanceFamily: String)

    val request = GetProductsRequest.builder()
      .serviceCode(EmrServiceCode)
      .filters(
        Filter.builder().`type`("TERM_MATCH").field("regionCode").value(region).build(),
        Filter.builder().`type`("TERM_MATCH").field("productFamily").value(EmrProductFamily).build(),
        Filter.builder().`type`("TERM_MATCH").field("softwareType").value(EmrSoftwareType).build(),
      ).build()
    val response = client.getProductsPaginator(request)
    val json = parse(response
      .stream()
      .flatMap(r => r.priceList().stream())
      .iterator()
      .asScala.toList
      .mkString("[", ",", "]")
    )
    json.extract[List[EmrService]].map { e =>
      val price = (e.terms \\ DefaultCurrency).values.toString
      EmrInstancePrice(
        e.product.attributes.instanceType,
        e.product.attributes.instanceFamily,
        price.toDouble)
    }.sortBy(r => (r.instanceFamily, r.instanceType))
  }

  /**
   * Generate a list of EMR Serverless costs by architecture type (x86_64, arm)
   *
   * @param region AWS Region that will be used. If none is specified, default to us-east-1 (N. Virginia)
   * @return List[EmrServerlessPrice]
   */
  def getEmrServerlessPrice(region: String = Region.US_EAST_1.toString): List[EmrServerlessPrice] = {

    def getServerlessProd(metric: String): Double = {
      case class ServiceProduct(product: Product, terms: JObject)
      case class Product(attributes: Attributes)
      case class Attributes(usagetype: String, meterunit: String)

      val request = GetProductsRequest.builder()
        .serviceCode(EmrServiceCode)
        .filters(
          Filter.builder().`type`("TERM_MATCH").field("regionCode").value(region).build(),
          Filter.builder().`type`("TERM_MATCH").field("meterunit").value(metric).build()
        ).build()
      val product = parse(client.getProductsPaginator(request).stream()
        .flatMap(r => r.priceList().stream())
        .iterator()
        .asScala.toList
        .mkString("[", ",", "]"))
        .extract[List[ServiceProduct]]
        .filter(_.product.attributes.usagetype.contains("SERVERLESS"))
      product.headOption.map(t => (t.terms \\ DefaultCurrency).values.toString.toDouble).getOrElse(0)
    }

    val armCpuPrice = getServerlessProd("ARMCPUHours")
    val armMemPrice = getServerlessProd("ARMGBHours")
    val x86CpuPrice = getServerlessProd("CPUHours")
    val x86MemPrice = getServerlessProd("GBHours")
    val storagePrice = getServerlessProd("StorageGBHours")

    List(
      EmrServerlessPrice(ArchitectureType.ARM64, armCpuPrice, armMemPrice, storagePrice),
      EmrServerlessPrice(ArchitectureType.X86_64, x86CpuPrice, x86MemPrice, storagePrice)
    ).filter(_.CPUHoursPrice != 0)

  }

  /**
   * Retrieve details on EC2 Service Costs for all instances in the specified Region.
   * Note: the guy who invented the schema for AWS pricing is a fucking idiot and should burn in hell.
   *
   * @param region AWS Region that will be used. If none is specified, default to us-east-1 (N. Virginia)
   * @return List[Ec2InstancePrice]
   */
  private def getEc2ServicePrice(region: String = Region.US_EAST_1.toString): List[Ec2InstancePrice] = {

    case class Ec2Service(product: Ec2Product, serviceCode: String, terms: JObject, publicationDate: String)
    case class Ec2Product(productFamily: String, attributes: Ec2Attributes)
    case class Ec2Attributes
    (memory: String,
      vcpu: String,
      storage: String,
      instanceFamily: String,
      operatingSystem: String,
      regionCode: String,
      physicalProcessor: String,
      networkPerformance: String,
      vpcnetworkingsupport: String,
      instanceType: String,
      currentGeneration: String,
      processorArchitecture: String,
      marketoption: String)

    val request = GetProductsRequest.builder()
      .serviceCode(Ec2ServiceCode)
      .filters(
        Filter.builder().`type`("TERM_MATCH").field("regionCode").value(region).build(),
        Filter.builder().`type`("TERM_MATCH").field("operatingSystem").value("Linux").build(),
        Filter.builder().`type`("TERM_MATCH").field("tenancy").value("Shared").build(),
        Filter.builder().`type`("TERM_MATCH").field("preInstalledSw").value("NA").build(),
        Filter.builder().`type`("TERM_MATCH").field("capacitystatus").value("Used").build(),
      ).build()
    val response = client.getProductsPaginator(request)
    val json = parse(response
      .stream()
      .flatMap(r => r.priceList().stream())
      .iterator()
      .asScala.toList
      .mkString("[", ",", "]"))

    json.extract[List[Ec2Service]].map { e =>
      val onDemandPrice = (e.terms \\ "OnDemand" \\ DefaultCurrency).values.toString
      val memoryGiB = ("""\d+""".r findFirstIn e.product.attributes.memory).getOrElse("0").toInt
      val currentGeneration = e.product.attributes.currentGeneration match {
        case "No" => false
        case "Yes" => true
      }

      // Disk Specification
      val volumeType: VolumeType = e.product.attributes.storage match {
        case x if x.toLowerCase.contains("ebs") => VolumeType.EBS
        case x if x.toLowerCase.contains("nvme") => VolumeType.NVME
        case x if x.toLowerCase.contains("ssd") => VolumeType.SSD
        case x if x.toLowerCase.contains("hdd") => VolumeType.HDD
        case _ =>
          if (e.product.attributes.instanceFamily.equalsIgnoreCase("storage optimized")) VolumeType.NVME
          else VolumeType.EBS
      }

      // For EBS volumes we use the EMR Defaults
      // https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-storage.html#emr-plan-storage-ebs-storage-default
      val (volumeNumber, volumeSize) = if (volumeType.equals(VolumeType.EBS)) {
        val instanceSize = e.product.attributes.instanceType.split('.')(1)
        val spec = EmrEbsDefaults.getOrElse(instanceSize, EmrEbsDefaults("default"))
        (spec.number, spec.sizeGiB)
      } else {
        val pattern = """(\d+) ?x? ?(\d+)? .*""".r
        val m = pattern.findAllIn(e.product.attributes.storage)
        // Group 2 might be null if string is: 150 GB NVMe SSD (1 disk implicit)
        if (m.group(2) != null) (m.group(1).toInt, m.group(2).toInt)
        else (1, m.group(1).toInt)
      }

      // Networking Specifications
      val netPattern = """(\d+) ([a-zA-Z]+)""".r
      val networkBandwidthGbps = e.product.attributes.networkPerformance match {
        case netPattern(x, y) =>
          if (y.toLowerCase.startsWith("mega")) x.toInt / 1024
          else x.toInt
        case _ => // Return a default of 10Gbps
          10
      }

      Ec2InstancePrice(
        e.product.attributes.instanceType,
        e.product.attributes.instanceFamily,
        memoryGiB,
        e.product.attributes.vcpu.toInt,
        e.product.attributes.processorArchitecture,
        onDemandPrice.toDouble,
        currentGeneration,
        volumeType,
        volumeNumber,
        volumeSize,
        networkBandwidthGbps)
    }.sortBy(r => (r.instanceFamily, r.instanceType))

  }

  /**
   * Generate a list of available Instances, with related details (memory, vCpu, storage), that can be used to
   * launch an Amazon EMR cluster in the specified AWS Region.
   *
   * @param region AWS Region that will be used. If none is specified, default to us-east-1 (N. Virginia)
   * @return List[Ec2InstancePrice]
   */
  def getEmrAvailableInstances(region: String = Region.US_EAST_1.toString): List[EmrInstance] = {

    val ec2Product = getEc2ServicePrice(region).sortBy(_.instanceType)
    val emrProduct = getEmrServicePrice(region).sortBy(_.instanceType)

    val commonProducts = emrProduct.map(_.instanceType).intersect(ec2Product.map(_.instanceType))
    val ec2Instances = ec2Product.filter(i => commonProducts.contains(i.instanceType)).sortBy(_.instanceType)
    val emrInstances = emrProduct.filter(i => commonProducts.contains(i.instanceType)).sortBy(_.instanceType)

    (emrInstances, ec2Instances).zipped.toList.map { case (emr, ec2) =>

      val totalMB = 1024.0 * ec2.memoryGiB
      val availableMemoryMB = totalMB - Math.round(Math.min(8192, Math.max(2048, 0.25 * totalMB)))
      val instanceFamily = ec2.instanceType.split('.')(0)

      EmrInstance(
        ec2.instanceType,
        instanceFamily,
        ec2.currentGeneration,
        ec2.vCpu,
        ec2.memoryGiB,
        ec2.volumeType,
        ec2.volumeNumber,
        ec2.volumeSizeGB,
        ec2.networkBandwidthGbps,
        availableMemoryMB.toInt,
        ec2.price,
        emr.price)
    }
  }

  /**
   * Generate a list of EMR on EKS costs by architecture type (EC2 or Fargate)
   *
   * @param region AWS Region that will be used. If none is specified, default to us-east-1 (N. Virginia)
   * @return List[EmrContainersPrice]
   */
  def getEmrContainersPrice(region: String = Region.US_EAST_1.toString): List[EmrContainersPrice] = {

    case class EmrOnEksPrice(product: EmrOnEksProduct, serviceCode: String, terms: JObject, publicationDate: String)
    case class EmrOnEksProduct(productFamily: String, attributes: EmrOnEksAttributes)
    case class EmrOnEksAttributes(regionCode: String, servicecode: String, meterunit: String, computeprovider: String)

    val request = GetProductsRequest.builder()
      .serviceCode(EmrServiceCode)
      .filters(
        Filter.builder().`type`("TERM_MATCH").field("regionCode").value(region).build(),
        Filter.builder().`type`("TERM_MATCH").field("productFamily").value(EmrContainersFamily).build(),
        Filter.builder().`type`("TERM_MATCH").field("operation").value(EmrContainersOperation).build(),
      ).build()
    val response = client.getProductsPaginator(request)
    val json = parse(response
      .stream()
      .flatMap(r => r.priceList().stream())
      .iterator()
      .asScala.toList
      .mkString("[", ",", "]"))

    val prods = json.extract[List[EmrOnEksPrice]]

    val ec2Price = prods.filter(_.product.attributes.computeprovider.equalsIgnoreCase(ComputeType.EC2.toString))
    val ec2CPUHours = (ec2Price
      .filter(_.product.attributes.meterunit.equalsIgnoreCase("CPUHours"))
      .head.terms \\ DefaultCurrency).values.toString.toDouble
    val ec2GBHours = (ec2Price
      .filter(_.product.attributes.meterunit.equalsIgnoreCase("GBHours"))
      .head.terms \\ DefaultCurrency).values.toString.toDouble

    val fargatePrice = prods.filter(_.product.attributes.computeprovider.equalsIgnoreCase(ComputeType.FARGATE.toString))
    val fargateCPUHours = (fargatePrice
      .filter(_.product.attributes.meterunit.equalsIgnoreCase("CPUHours"))
      .head.terms \\ DefaultCurrency).values.toString.toDouble
    val fargateGBHours = (fargatePrice
      .filter(_.product.attributes.meterunit.equalsIgnoreCase("GBHours"))
      .head.terms \\ DefaultCurrency).values.toString.toDouble

    List(
      EmrContainersPrice(ComputeType.EC2, ec2GBHours, ec2CPUHours),
      EmrContainersPrice(ComputeType.FARGATE, fargateGBHours, fargateCPUHours)
    )
  }

  /**
   * Generate a list of EBS costs
   *
   * @param region AWS Region that will be used. If none is specified, default to us-east-1 (N. Virginia)
   * @return List[EbsPrice]
   */
  def getEbsServicePrice(region: String = Region.US_EAST_1.toString): List[EbsPrice] = {

    case class EbsService(product: EbsProduct, serviceCode: String, terms: JObject, publicationDate: String)
    case class EbsProduct(productFamily: String, attributes: EbsAttributes)
    case class EbsAttributes(regionCode: String, volumeApiName: String, maxThroughputvolume: String, maxIopsvolume: String, maxVolumeSize: String, storageMedia: String, volumeType: String)

    val request = GetProductsRequest.builder()
      .serviceCode(Ec2ServiceCode)
      .filters(
        Filter.builder().`type`("TERM_MATCH").field("regionCode").value(region).build(),
        Filter.builder().`type`("TERM_MATCH").field("productFamily").value("Storage").build()
      ).build()
    val response = client.getProductsPaginator(request)
      .stream()
      .flatMap(r => r.priceList().stream())
      .iterator()
      .asScala.toList
      .mkString("[", ",", "]")

    parse(response).extract[List[EbsService]].map { e =>
      val onDemandPrice = (e.terms \\ "OnDemand" \\ DefaultCurrency).values.toString
      EbsPrice(e.product.attributes.volumeApiName, onDemandPrice.toDouble)
    }

  }

}