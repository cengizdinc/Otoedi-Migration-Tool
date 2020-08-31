<?php

/**
 * PHP Version >= PHP 7.4
 *
 * @category Migration
 * @package  OtoediMigration
 * @author   Serkan Mert Kaptan <serkan.kaptan@map.com.tr>
 * @license  MAP E-Commerce and Data Services Inc.
 * @link     https://www.map.com.tr
 * @see      http://jira.map.com.tr:8090/browse/OT-157
 */

namespace Otoedi\Migration;

require_once __DIR__ . '/vendor/autoload.php';

use Laminas\Db\Adapter\Exception\InvalidQueryException;
use Laminas\Db\Sql\Expression;
use Laminas\Db\Sql\Select;
use Laminas\Db\Sql\Where;
use Otoedi\Migration\Helper\Db;
use Map\Logger\EchoLogger;


$logger = new EchoLogger();

echo "Please type source party code to be migrated: ";
$otoediCode = trim(fgets(STDIN));

// ---------------------------------------------------------------------------------------------------------------------

$dbConfig = json_decode(file_get_contents(__DIR__ . "/config/db.config.json"));
!empty($dbConfig) or die('Can not be found the configuration parameters!');

$instanceS = new Db((array)$dbConfig->source);
$instanceT = new Db((array)$dbConfig->target);

// ---------------------------------------------------------------------------------------------------------------------

$partyWhere = new Where();
$partyWhere->nest()
    ->equalTo("PS.EDI_CODE", $otoediCode)
    ->or
    ->equalTo("PR.EDI_CODE", $otoediCode)
    ->unnest();
$relations = $instanceS->get(
    "PARTIES_PARTIES",
    "PP",
    [],
    $partyWhere,
    false,
    false,
    [
        ["name" => ["PS" => "PARTIES"], "on" => "PS.ID = PP.SENDER_PARTIES_ID", "columns" => ["senderName" => "NAME", "senderCode" => "EDI_CODE"], "type" => Select::JOIN_LEFT],
        ["name" => ["PR" => "PARTIES"], "on" => "PR.ID = PP.RECEPIENT_PARTIES_ID", "columns" => ["receiverName" => "NAME", "receiverCode" => "EDI_CODE"], "type" => Select::JOIN_LEFT],
    ]
);
$connectionT = $instanceT->adapter->getDriver()->getConnection();
foreach ($relations as $relation) {
    echo PHP_EOL . PHP_EOL . PHP_EOL . PHP_EOL;
    switch ($relation["XDOC_TYPE_ID"]) {
        case "1":
        case "2":
            $relation["supplierCode"] = $relation["receiverCode"];
            $relation["supplierName"] = $relation["receiverName"];
            $relation["buyerCode"] = $relation["senderCode"];
            $relation["buyerName"] = $relation["senderName"];
            break;
        case "3":
            $relation["supplierCode"] = $relation["senderCode"];
            $relation["supplierName"] = $relation["senderName"];
            $relation["buyerCode"] = $relation["receiverCode"];
            $relation["buyerName"] = $relation["receiverName"];
            break;
    }

    $supplierInformation = $instanceT->get("party", "p", [], ["otoedi_code" => $relation["supplierCode"], "type" => 2]);
    if (empty($supplierInformation)) {
        $logger->info("Supplier {$relation["supplierCode"]}, {$relation["supplierName"]} doesn't exist at target database. Creating.. ");
        $supplierId = $instanceT->create("party", [
            "identifier" => $relation["supplierCode"],
            "otoedi_code" => $relation["supplierCode"],
            "type" => 2,
            "name" => mb_substr($relation["supplierName"], 0, 50)
        ]);
        $supplierInformation["party_id"] = $supplierId;
        $logger->info("Supplier party created with following row id, $supplierId.");
    }

    $buyerInformation = $instanceT->get("party", "p", [], ["otoedi_code" => $relation["buyerCode"], "type" => 1]);
    if (empty($buyerInformation)) {
        $logger->error("Buyer {$relation["buyerCode"]}, {$relation["buyerName"]} doesn't exist at target database. Creating.");
        $buyerPartyId = $instanceT->create("party", [
            "identifier" => $relation["buyerCode"],
            "otoedi_code" => $relation["buyerCode"],
            "type" => 1,
            "name" => mb_substr($relation["buyerName"], 0, 50)
        ]);
        $buyerInformation["party_id"] = $buyerPartyId;
        $logger->info("Related buyer party created with following row id, $buyerPartyId.");
    }

    $pr = $instanceT->get("party_relation", "pr", [], [
        "fk_buyer_id" => $buyerInformation["party_id"],
        "fk_supplier_id" => $supplierInformation["party_id"]
    ]);

    if (empty($pr)) {
        $prId = $instanceT->create("party_relation", [
            "fk_buyer_id" => $buyerInformation["party_id"],
            "fk_supplier_id" => $supplierInformation["party_id"]
        ]);
        $pr["pr_id"] = $prId;

        $logger->info("Party relation created with row id, $prId.");
    }

    $sellerInformation = $instanceT->get("party", "p", [], ["otoedi_code" => $relation["supplierCode"]]);
    if (empty($sellerInformation)) {
        $logger->error("Seller  {$relation["supplierCode"]} doesn't exist at target database. Creating.");
        $sellerPartyId = $instanceT->create("party", [
            "identifier" => $relation["supplierCode"],
            "otoedi_code" => $relation["supplierCode"],
            "type" => 4,
            "name" => $relation["NAME"]
        ]);
        $sellerInformation["party_id"] = $sellerPartyId;

        $logger->info("Related seller party created with following row id, $sellerPartyId.");
    }

    // ---------------------------------------------------------------------------------------------------------
    $xdocWhere = new Where();
    $xdocWhere->nest()
        ->equalTo("PS.EDI_CODE", $otoediCode)
        ->or
        ->equalTo("PR.EDI_CODE", $otoediCode)
        ->unnest();
    $xdocWhere->equalTo("X.XDOC_TYPE_ID", $relation["XDOC_TYPE_ID"]);

    $getXdocList = $instanceS->get("XDOC", "X", [], $xdocWhere, 5, "X.XDOC_TYPE_ID", [
        ["name" => ["XT" => "XDOC_TYPE"], "on" => "XT.ID = X.XDOC_TYPE_ID", "columns" => ["TYPE"], "type" => Select::JOIN_LEFT],
        ["name" => ["PS" => "PARTIES"], "on" => "X.SENDER_PARTY_ID = PS.ID", "columns" => [], "type" => Select::JOIN_LEFT],
        ["name" => ["PR" => "PARTIES"], "on" => "X.RECEPIENT_PARTY_ID = PR.ID", "columns" => [], "type" => Select::JOIN_LEFT],
    ], "X.ID");

    if (empty($getXdocList)) {
        $logger->error("Couldnt find any document with type of {$relation["XDOC_TYPE_ID"]}.");
        continue;
    }

    if (!isset($getXdocList[0])) {
        $getXdocList = [$getXdocList];
    }

    foreach ($getXdocList as $xdoc) {
        echo PHP_EOL . PHP_EOL;
        $connectionT->beginTransaction();
        if (empty($xdoc["TYPE"])) {
            $logger->error("Cannot identify document type, xdoc id: {$xdoc["ID"]}. Skipping.");
            $connectionT->rollback();
            continue;
        }
        try {
            $documentId = $instanceT->create("document", [
                "fk_pr_id" => $pr["pr_id"],
                "fk_dt_id" => in_array($xdoc["XDOC_TYPE_ID"], [1, 2]) ? 1 : 2,
                "type" => $xdoc["TYPE"],
                "number" => $xdoc["RELEASE_NUMBER"],
                "control_reference" => $xdoc["RELEASE_NUMBER"],
                "datetime" => $xdoc["ISSUE_DATE"],
                "additional_information" => '{"migratedFromV2": "yes","validityPeriod": {"from": "", "until": ""}, "sender_edi_code": "'
                    . $buyerInformation["party_id"] . '", "receiver_edi_code": "'
                    . $supplierInformation["party_id"] . '"}',
                "original_filename" => $xdoc["XML_PATH"],
                "insert_date" => $xdoc["INSERT_TIME"],
            ]);

            $logger->info("Document created, document id: $documentId, xdoc id: {$xdoc["ID"]}");
        } catch (InvalidQueryException $e) {
            $logger->error("Cannot create document, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
            continue;
        }
        switch ($xdoc["TYPE"]) {
            case "DELFOR":
                $delforList = $instanceS->get("XDOC_DELFOR_DETAIL", "XDD", [], ["XDD.XDOC_ID" => $xdoc["ID"]], false, false, [
                    ["name" => ["XD" => "XDOC_DELFOR"], "on" => "XDD.DELFOR_ID = XD.ID", "columns" => ["Snrf", "BeginningInventoryDate", "HorizonEndDate", "BuyerCode", "SupplierCode", "SellerCode"], "type" => Select::JOIN_LEFT],
                    ["name" => ["X" => "XDOC"], "on" => "XD.XDOC_ID = X.ID", "columns" => ["ISSUE_DATE",], "type" => Select::JOIN_LEFT]
                ]);

                if (!isset($delforList[0])) {
                    $delforList = [$delforList];
                }

                try {
                    $orderId = $instanceT->create("order", [
                        "fk_document_id" => $documentId,
                        "fk_pr_id" => $pr["pr_id"],
                        "order_number" => !empty($delforList[0]["Snrf"]) ? $delforList[0]["Snrf"] : $xdoc["RELEASE_NUMBER"],
                        "order_date" => $xdoc["ISSUE_DATE"],
                        "horizon_start_date" => $delforList[0]["BeginningInventoryDate"] == "0000-00-00" ? null : @$delforList[0]["BeginningInventoryDate"],
                        "horizon_end_date" => $delforList[0]["HorizonEndDate"] == "0000-00-00" ? null : @$delforList[0]["HorizonEndDate"],
                        "fk_buyer_id" => $buyerInformation["party_id"],
                        "buyer_identifier" => @$delforList[0]["BuyerCode"],
                        "fk_supplier_id" => $supplierInformation["party_id"],
                        "supplier_identifier" => @$delforList[0]["SupplierCode"],
                        "fk_seller_id" => $supplierInformation["party_id"],
                        "seller_identifier" => @$delforList[0]["SellerCode"],
                        "insert_date" => $xdoc["INSERT_TIME"],
                    ]);

                    $logger->info("Order created, order id: $orderId.");
                } catch (InvalidQueryException $e) {
                    $logger->error("Cannot create Order, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                    $connectionT->rollback();
                    continue 2;
                }

                foreach ($delforList as $i => $line) {
                    $consignee = $instanceT->get("consignee", "c", [], ["identifier" => $line["DeliveryPointCode"], "fk_buyer_id" => $buyerInformation["party_id"]]);
                    if (empty($consignee)) {
                        try {
                            $consigneeId = $instanceT->create("consignee", [
                                "fk_buyer_id" => $buyerInformation["party_id"],
                                "identifier" => $line["DeliveryPointCode"],
                                "name" => $line["DeliveryPointCode"],
                            ]);
                            $consignee["consignee_id"] = $consigneeId;

                            $logger->info("Consignee created, consignee id: $consigneeId.");
                        } catch (InvalidQueryException $e) {
                            $logger->error("Cannot create consignee, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                            $connectionT->rollback();
                            continue 3;
                        }
                    }

                    if (!empty($line["UnloadingDockCode"])) {
                        $dock = $instanceT->get("dock", "d", ["identifier" => $line["UnloadingDockCode"], "fk_buyer_id" => $buyerInformation["party_id"]]);
                        if (empty($dock)) {
                            try {
                                $dockId = $instanceT->create("consignee", [
                                    "fk_buyer_id" => $buyerInformation["party_id"],
                                    "fk_consignee_id" => $consignee["consignee_id"],
                                    "identifier" => $line["UnloadingDockCode"],
                                    "name" => $line["UnloadingDockCode"],
                                ]);
                                $dock["dock_id"] = $dockId;

                                $logger->info("Dock created, dock id: $dockId.");
                            } catch (InvalidQueryException $e) {
                                $logger->error("Cannot create dock, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                                $connectionT->rollback();
                                continue 3;
                            }
                        }
                    }

                    $orderConsignee = $instanceT->get("order_consignee", "oc", [], [
                        "fk_order_id" => $orderId,
                        "fk_consignee_id" => $consignee["consignee_id"],
                        "fk_dock_id" => $dock["dock_id"] ?? null,
                        "consignee_identifier" => $line["DeliveryPointCode"],
                        "dock_identifier" => !empty($line["UnloadingDockCode"]) ? $line["UnloadingDockCode"] : $line["DeliveryPointCode"]
                    ]);
                    if (empty($orderConsignee)) {
                        try {
                            $ocId = $instanceT->create("order_consignee", [
                                "fk_order_id" => $orderId,
                                "fk_consignee_id" => $consignee["consignee_id"],
                                "fk_dock_id" => $dock["dock_id"] ?? null,
                                "consignee_identifier" => $line["DeliveryPointCode"],
                                "dock_identifier" => !empty($line["UnloadingDockCode"]) ? $line["UnloadingDockCode"] : $line["DeliveryPointCode"]
                            ]);
                            $orderConsignee["order_consignee_id"] = $ocId;

                            $logger->info("OC created, oc id: $ocId.");
                        } catch (InvalidQueryException $e) {
                            $logger->error("Cannot create oc, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                            $connectionT->rollback();
                            continue 3;
                        }
                    }

                    $product = $instanceT->get("product", "p", [], ["identifier" => "ItemSenderCode", "fk_supplier_id" => $supplierInformation["party_id"]]);
                    if (empty($product)) {
                        try {
                            $productId = $instanceT->create("product", [
                                "fk_supplier_id" => $supplierInformation["party_id"],
                                "identifier" => !empty($line["ItemSenderCode"]) ? $line["ItemSenderCode"] : $line["ItemReceiverCode"],
                                "description" => $line["ItemDescription"] ?? null
                            ]);
                            $product["product_id"] = $productId;

                            $logger->info("Product created, id: $productId.");
                        } catch (InvalidQueryException $e) {
                            $logger->error("Cannot create product, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                            $connectionT->rollback();
                            continue 3;
                        }
                    }
                    try {
                        $orderDetailId = $instanceT->create("order_line", [
                            "fk_order_consignee_id" => $orderConsignee["order_consignee_id"],
                            "fk_order_id" => $orderId,
                            "fk_product_id" => $product["product_id"],
                            "line_number" => $line["SchedulingConditionId"],
                            "release_number" => 0,
                            "identifier" => !empty($line["ItemSenderCode"]) ? $line["ItemSenderCode"] : $line["ItemReceiverCode"],
                            "description" => $line["ItemDescription"] ?? null,
                            "buyer_code" => $line["ItemSenderCode"],
                            "supplier_code" => $line["ItemReceiverCode"] ?? null,
                            "delivery_call_number" => null,
                            "contract_number" => null,
                            "earliest_datetime" => $line["ForecastPeriodStartDate"],
                            "latest_datetime" => $line["HorizonEndDate"] == "0000-00-00" ? null : $line["HorizonEndDate"],
                            "collection_datetime_earliest" => null,
                            "collection_datetime_latest" => null,
                            "last_despatch_datetime" => $line["LastAsnShipmentDate"] == "0000-00-00" ? null : $line["LastAsnShipmentDate"],
                            "order_status" => "forecast",
                            "quantity_original" => $line["ForecastNetQuantity"],
                            "quantity_confirmed" => !empty($line["approveduserid"]) ? $line["ForecastNetQuantity"] : 0,
                            "quantity_packing" => 0,
                            "quantity_packed" => 0,
                            "quantity_to_be_shipped" => 0,
                            "quantity_shipped" => $line["ForecastDeliveredQuantity"],
                            "quantity_invoiced" => 0,
                            "unit_quantity" => $line["ForecastNetQuantityUom"],
                            "unit_price" => 0,
                            "unit_price_basis" => null,
                            "line_amount" => null,
                            "unit_price_currency" => null,
                            "original_delivery_date" => $line["ForecastPeriodStartDate"],
                            "additional_information" => null,
                            "insert_date" => $xdoc["INSERT_TIME"],
                        ]);

                        $matchWithDesadv = $instanceS->get("DESADV_DELJIT", "DD", [], ["XDOC_DELFOR_DETAIL_ID" => $line["ID"]]);
                        if (!isset($matchWithDesadv[0])) {
                            $matchWithDesadv = [$matchWithDesadv];
                        }
                        foreach ($matchWithDesadv as $desadv) {
                            if (!empty($matchWithDesadv["XDOC_DESADV_DETAIL_ID"])) {
                                try {
                                    $instanceT->create("v2_migration", [
                                        "order_line_id" => $orderDetailId,
                                        "XDOC_DESADV_DETAIL_ID" => $desadv["XDOC_DESADV_DETAIL_ID"],
                                        "XDOC_DELFOR_DETAIL_ID" => $line["ID"],
                                        "consignee_identifier" => @$line["DeliveryPointCode"],
                                        "dock_identifier" => !empty($line["UnloadingDockCode"]) ? $line["UnloadingDockCode"] : @$line["DeliveryPointCode"],
                                        "original_delivery_date" => @$line["ForecastPeriodStartDate"],
                                    ]);
                                } catch (InvalidQueryException $e) {
                                    $logger->error("Cannot match with a desadv, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                                    $connectionT->rollback();
                                    continue 4;
                                }
                            }
                        }

                        $logger->info("Line created, id: $orderDetailId.");
                        echo PHP_EOL;
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create line, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                        $connectionT->rollback();
                        continue 3;
                    }
                }
                break;
            case "DELJIT":
                $deljitList = $instanceS->get(
                    "XDOC_DELJIT_DETAIL",
                    "XDD",
                    [],
                    ["XDD.XDOC_ID" => $xdoc["ID"]],
                    false,
                    false,
                    [
                        ["name" => ["XD" => "XDOC_DELJIT"], "on" => "XDD.DELJIT_ID = XD.ID", "columns" => ["Snrf", "HorizonStartDate", "HorizonEndDate", "BuyerCode", "SupplierCode", "SellerCode", "ShipToCode"], "type" => Select::JOIN_LEFT],
                        ["name" => ["X" => "XDOC"], "on" => "XD.XDOC_ID = X.ID", "columns" => [], "type" => Select::JOIN_LEFT],
                    ]
                );

                if (!isset($deljitList[0])) {
                    $deljitList = [$deljitList];
                }

                try {
                    $orderId = $instanceT->create("order", [
                        "fk_document_id" => $documentId,
                        "fk_pr_id" => $pr["pr_id"],
                        "order_number" => !empty($deljitList[0]["PurchaseOrderNumber"]) ? $deljitList[0]["PurchaseOrderNumber"] : $xdoc["RELEASE_NUMBER"],
                        "order_date" => $xdoc["ISSUE_DATE"] != "0000-00-00" ? $xdoc["ISSUE_DATE"] : null,
                        "horizon_start_date" => $deljitList[0]["HorizonStartDate"] != "0000-00-00" ? @$deljitList[0]["HorizonStartDate"] : null,
                        "horizon_end_date" => $deljitList[0]["HorizonEndDate"] != "0000-00-00" ? @$deljitList[0]["HorizonEndDate"] : null,
                        "fk_buyer_id" => $buyerInformation["party_id"],
                        "buyer_identifier" => @$deljitList[0]["BuyerCode"],
                        "fk_supplier_id" => $supplierInformation["party_id"],
                        "supplier_identifier" => @$deljitList[0]["SupplierCode"],
                        "fk_seller_id" => $supplierInformation["party_id"],
                        "seller_identifier" => @$deljitList[0]["SellerCode"],
                        "insert_date" => $xdoc["INSERT_TIME"],
                    ]);

                    $logger->info("Order created, order id: $orderId.");
                } catch (InvalidQueryException $e) {
                    $logger->error("Cannot create Order, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                    $connectionT->rollback();
                    continue 2;
                }

                foreach ($deljitList as $line) {
                    $consignee = $instanceT->get("consignee", "c", [], ["identifier" => $deljitList[0]["ShipToCode"], "fk_buyer_id" => $buyerInformation["party_id"]]);
                    if (empty($consignee)) {
                        try {
                            $consigneeId = $instanceT->create("consignee", [
                                "fk_buyer_id" => $buyerInformation["party_id"],
                                "identifier" => $deljitList[0]["ShipToCode"],
                                "name" => $deljitList[0]["ShipToCode"],
                            ]);
                            $consignee["consignee_id"] = $consigneeId;

                            $logger->info("Consignee created, consignee id: $consigneeId.");
                        } catch (InvalidQueryException $e) {
                            $logger->error("Cannot create consignee, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                            $connectionT->rollback();
                            continue 3;
                        }
                    }

                    if (!empty($deljitList["UnloadingDockCode"])) {
                        $dock = $instanceT->get("dock", "d", [], ["identifier" => $line["UnloadingDockCode"], "fk_buyer_id" => $buyerInformation["party_id"]]);
                        if (empty($dock)) {
                            try {
                                $dockId = $instanceT->create("consignee", [
                                    "fk_buyer_id" => $buyerInformation["party_id"],
                                    "fk_consignee_id" => $consignee["consignee_id"],
                                    "identifier" => $line["UnloadingDockCode"],
                                    "name" => $line["UnloadingDockCode"],
                                ]);
                                $dock["dock_id"] = $dockId;

                                $logger->info("Dock created, dock id: $dockId.");
                            } catch (InvalidQueryException $e) {
                                $logger->error("Cannot create dock, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                                $connectionT->rollback();
                                continue 3;
                            }
                        }
                    }

                    $orderConsignee = $instanceT->get("order_consignee", "oc", [], [
                        "fk_order_id" => $orderId,
                        "fk_consignee_id" => $consignee["consignee_id"],
                        "fk_dock_id" => $dock["dock_id"] ?? null,
                        "consignee_identifier" => !empty($line["ShipToCode"]) ? $line["ShipToCode"] : $relation["buyerCode"],
                        "dock_identifier" => !empty($line["ShipToCode"]) ? $line["ShipToCode"] : $relation["buyerCode"],
                    ]);

                    if (empty($orderConsignee)) {
                        try {
                            $ocId = $instanceT->create("order_consignee", [
                                "fk_order_id" => $orderId,
                                "fk_consignee_id" => $consignee["consignee_id"],
                                "fk_dock_id" => $dock["dock_id"] ?? null,
                                "consignee_identifier" => !empty($line["ShipToCode"]) ? $line["ShipToCode"] : $relation["buyerCode"],
                                "dock_identifier" => !empty($line["ShipToCode"]) ? $line["ShipToCode"] : $relation["buyerCode"],
                            ]);
                            $orderConsignee["order_consignee_id"] = $ocId;

                            $logger->info("OC created, oc id: $ocId.");
                        } catch (InvalidQueryException $e) {
                            $logger->error("Cannot create order consignee, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                            $connectionT->rollback();
                            continue 3;
                        }
                    }

                    $product = $instanceT->get("product", "p", [], ["identifier" => "ItemSenderCode", "fk_supplier_id" => $supplierInformation["party_id"]]);
                    if (empty($product)) {
                        try {
                            $productId = $instanceT->create("product", [
                                "fk_supplier_id" => $supplierInformation["party_id"],
                                "identifier" => !empty($line["ItemSenderCode"]) ? $line["ItemSenderCode"] : @$line["ItemReceiverCode"],
                                "description" => $line["ItemDescription"] ?? $line["ItemSenderCode"]
                            ]);
                            $product["product_id"] = $productId;
                            $logger->info("Product created, id: $productId.");
                        } catch (InvalidQueryException $e) {
                            $logger->error("Cannot create product, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                            $connectionT->rollback();
                            continue 3;
                        }
                    }

                    try {
                        $orderDetailId = $instanceT->create("order_line", [
                            "fk_order_consignee_id" => $orderConsignee["order_consignee_id"],
                            "fk_order_id" => $orderId,
                            "fk_product_id" => $product["product_id"],
                            "line_number" => $line["SchedulingConditionId"],
                            "release_number" => "0",
                            "identifier" => !empty($line["ItemSenderCode"]) ? $line["ItemSenderCode"] : $line["ItemReceiverCode"],
                            "description" => $line["ItemDescription"] ?? null,
                            "buyer_code" => $line["ItemSenderCode"],
                            "supplier_code" => $line["ItemReceiverCode"] ?? null,
                            "delivery_call_number" => null,
                            "contract_number" => null,
                            "earliest_datetime" => $line["ShipScheduleDate"],
                            "latest_datetime" => $deljitList[0]["HorizonEndDate"] == "0000-00-00" ? null : $deljitList[0]["HorizonEndDate"],
                            "collection_datetime_earliest" => null,
                            "collection_datetime_latest" => null,
                            "last_despatch_datetime" => $line["LastAsnShipmentDate"] == "0000-00-00" ? null : $line["LastAsnShipmentDate"],
                            "order_status" => "firm",
                            "quantity_original" => $line["ScheduleQuantity"],
                            "quantity_confirmed" => !empty($line["approveduserid"]) ? $line["ScheduleQuantity"] : 0,
                            "quantity_packing" => 0,
                            "quantity_packed" => 0,
                            "quantity_to_be_shipped" => 0,
                            "quantity_shipped" => $line["DeliveredQuantity"],
                            "quantity_invoiced" => 0,
                            "unit_quantity" => $line["ScheduleQuantityUom"],
                            "unit_price" => 0,
                            "unit_price_basis" => null,
                            "line_amount" => null,
                            "unit_price_currency" => null,
                            "original_delivery_date" => $line["ShipScheduleDate"],
                            "additional_information" => null,
                            "insert_date" => $xdoc["INSERT_TIME"],
                        ]);

                        $matchWithDesadv = $instanceS->get("DESADV_DELJIT", "DD", [], ["XDOC_DELJIT_DETAIL_ID" => $line["ID"]]);
                        if (!isset($matchWithDesadv[0])) {
                            $matchWithDesadv = [$matchWithDesadv];
                        }
                        foreach ($matchWithDesadv as $desadv) {
                            if (!empty($desadv["XDOC_DESADV_DETAIL_ID"])) {
                                try {
                                    $instanceT->create("v2_migration", [
                                        "order_line_id" => $orderDetailId,
                                        "XDOC_DESADV_DETAIL_ID" => $desadv["XDOC_DESADV_DETAIL_ID"],
                                        "XDOC_DELJIT_DETAIL_ID" => $line["ID"],
                                        "consignee_identifier" => @$line["DeliveryPointCode"],
                                        "dock_identifier" => !empty($line["UnloadingDockCode"]) ? $line["UnloadingDockCode"] : @$line["DeliveryPointCode"],
                                        "original_delivery_date" => @$line["ShipScheduleDate"],
                                    ]);
                                } catch (InvalidQueryException $e) {
                                    $logger->error("Cannot match with a desadv, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                                    $connectionT->rollback();
                                    continue 4;
                                }
                            }
                        }

                        $logger->info("Line created, id: $orderDetailId.");
                        echo PHP_EOL;
                    } catch (InvalidQueryException $e) {
                        $logger->info("Cannot create order line, id: {$xdoc["ID"]}. Error: {$e->getMessage()}");
                        $connectionT->rollback();
                        continue 3;
                    }
                }
                break;
            case "DESADV":
                $despatchList = $instanceS->get("XDOC_DESADV_DETAIL", "XDD", [], ["XDD.XDOC_ID" => $xdoc["ID"]], false, false, [
                    ["name" => ["XD" => "XDOC_DESADV"], "on" => "XDD.DESADV_ID = XD.ID", "columns" => ["CarrierName", "ModeOfTransport", "IntermediateConsigneeCode", "FreightBillNumber", "ShipToCode", "ShipmentNumber", "BillOfLadingNumber", "ShipmentDateTime", "EstimatedArrivalDateTime", "TotalGrossWeight", "TotalNetWeight", "TotalGrossWeightUom"], "type" => Select::JOIN_LEFT],
                    ["name" => ["X" => "XDOC"], "on" => "XD.XDOC_ID = X.ID", "columns" => ["STATUS"], "type" => Select::JOIN_LEFT],
                ]);

                if (!isset($despatchList[0])) {
                    $despatchList = [$despatchList];
                }

                $consignee = $instanceT->get("consignee", "c", [], ["identifier" => $despatchList[0]["ShipToCode"], "fk_buyer_id" => $buyerInformation["party_id"]]);
                if (empty($consignee)) {
                    try {
                        $consigneeId = $instanceT->create("consignee", [
                            "fk_buyer_id" => $buyerInformation["party_id"],
                            "identifier" => $despatchList[0]["ShipToCode"],
                            "name" => $despatchList[0]["ShipToCode"],
                        ]);
                        $consignee["consignee_id"] = $consigneeId;

                        $logger->info("Consignee created, consignee id: $consigneeId.");
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create consignee, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                        $connectionT->rollback();
                        continue 2;
                    }
                }

                $dock = $instanceT->get("dock", "d", [], ["identifier" => $despatchList[0]["ShipToCode"], "fk_buyer_id" => $buyerInformation["party_id"]]);
                if (empty($dock) and !empty($despatchList[0]["ShipToCode"])) {
                    try {
                        $dockId = $instanceT->create("dock", [
                            "fk_buyer_id" => $buyerInformation["party_id"],
                            "fk_consignee_id" => $consignee["consignee_id"],
                            "identifier" => $despatchList[0]["ShipToCode"],
                            "name" => $despatchList[0]["ShipToCode"],
                        ]);
                        $dock["dock_id"] = $dockId;

                        $logger->info("Dock created, dock id: $dockId.");
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create dock, {$e->getMessage()}; xdoc id: {$xdoc["ID"]}. Skipping.");
                        $connectionT->rollback();
                        continue 2;
                    }
                }

                try {
                    $despatchId = $instanceT->create("despatch", [
                        "fk_pr_id" => $pr["pr_id"],
                        "fk_shipment_id" => null,
                        "fk_invoicee_id" => null,
                        "fk_consignee_id" => $consignee["consignee_id"],
                        "fk_dock_id" => $dock["dock_id"],
                        "fk_document_id" => $documentId,
                        "despatch_number" => $despatchList[0]["ShipmentNumber"],
                        "bill_of_lading_number" => $despatchList[0]["BillOfLadingNumber"],
                        "despatch_date" => $despatchList[0]["ShipmentDateTime"],
                        "arrival_date" => $despatchList[0]["EstimatedArrivalDateTime"],
                        "gross_weight" => $despatchList[0]["TotalGrossWeight"],
                        "net_weight" => $despatchList[0]["TotalNetWeight"],
                        "weight_unit" => $despatchList[0]["TotalGrossWeightUom"],
                        "number_of_packages" => null,
                        "insert_date" => $xdoc["INSERT_TIME"]
                    ]);

                    $logger->info("Despatch created, despatch id: $despatchId.");
                } catch (InvalidQueryException $e) {
                    $logger->error("Cannot create despatch, {$e->getMessage()} xdoc id: {$xdoc["ID"]}. Skipping.");
                    $connectionT->rollback();
                    continue 2;
                }

                foreach ($despatchList as $line) {
                    $product = $instanceT->get("product", "p", [], ["identifier" => "ItemSenderCode", "fk_supplier_id" => $supplierInformation["party_id"]]);
                    if (empty($product)) {
                        try {
                            $productId = $instanceT->create("product", [
                                "fk_supplier_id" => $supplierInformation["party_id"],
                                "identifier" => !empty($line["ItemSenderCode"]) ? $line["ItemSenderCode"] : $line["ItemReceiverCode"],
                                "description" => $line["ItemDescription"]
                            ]);
                            $product["product_id"] = $productId;

                            $logger->info("Product created, id: $productId.");
                        } catch (InvalidQueryException $e) {
                            $logger->error("Cannot create product, {$e->getMessage()}, xdoc id: {$xdoc["ID"]}. Skipping.");
                            $connectionT->rollback();
                            continue 3;
                        }
                    }

                    $migratedOrderLine = $instanceT->get("v2_migration", "m", [], ["XDOC_DESADV_DETAIL_ID" => $line["ID"]], 1);
                    try {
                        $dpId = $instanceT->create("despatch_product", [
                            "fk_despatch_id" => $despatchId,
                            "fk_order_line_id" => $migratedOrderLine["order_line_id"],
                            "fk_product_id" => $product["product_id"],
                            "quantity_despatch" => $line["DispatchQuantity"],
                            "quantity_package_handling" => 1,
                            "quantity_package_packaging" => 1,
                            "unit_quantity" => "PCE",
                            "advice_note_number" => "",
                            "insert_date" => $xdoc["INSERT_TIME"]
                        ]);
                        $instanceT->update("v2_migration", ["despatch_product_id" => $dpId], ["XDOC_DESADV_DETAIL_ID" => $line["ID"]]);

                        $logger->info("Despatch product created. id: $dpId");
                        echo PHP_EOL;
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create despatch product, {$e->getMessage()}, xdoc id: {$xdoc["ID"]}. Skipping.");
                        $connectionT->rollback();
                        continue 3;
                    }

                    $shipment = $instanceT->get("shipment", "s", [], ["transport_identifier" => $line["ShipmentNumber"]]);
                    if (empty($shipment)) {
                        try {
                            $shipmentId = $instanceT->create("shipment", [
                                "fk_party_id" => $supplierInformation["party_id"],
                                "fk_carrier_id" => null,
                                "is_shipped" => $line["STATUS"] & 8,
                                "carrier_name" => $line["CarrierName"],
                                "transport_identifier" => $line["ShipmentNumber"],
                                "transport_identifier_meaning" => 1,
                                "despatch_datetime" => $line["ShipmentDateTime"],
                                "arrival_datetime" => $line["EstimatedArrivalDateTime"],
                                "mode_of_transport" => $line["ModeOfTransport"],
                                "intermediate_consignee_code" => $line["IntermediateConsigneeCode"],
                                "gross_weight" => $line["TotalGrossWeight"],
                                "net_weight" => $line["TotalNetWeight"],
                                "weight_unit" => !empty($line["TotalGrossWeightUom"]) ? $line["TotalGrossWeightUom"] : $line["TotalNetWeightUom"],
                                "number_of_packages" => null,
                                "shipment_number" => $line["ShipmentNumber"],
                                "port_of_loading" => null,
                                "port_of_discharge" => null,
                                "shipping_mark1" => null,
                                "shipping_mark2" => null,
                                "shipping_mark3" => null,
                                "shipping_mark4" => null,
                                "use_system_despatch_date" => 0,
                                "freight_payment_code" => $line["FreightBillNumber"],
                                "freight_bill_number_details" => $line["FreightBillNumber"],
                                "insert_date" => $xdoc["INSERT_TIME"]
                            ]);
                            $shipment["shipment_id"] = $shipmentId;

                            $logger->info("Shipment created, id: $shipmentId");
                            echo PHP_EOL;
                        } catch (InvalidQueryException $e) {
                            $logger->error("Cannot create shipment, {$e->getMessage()}, xdoc id: {$xdoc["ID"]}. Skipping.");
                            $connectionT->rollback();
                            continue 3;
                        }
                    }
                }
                break;
        }
        $connectionT->commit();
    }

    $deljitShipmentCumulativeWhere = new Where();
    $deljitShipmentCumulativeWhere->equalTo("XDOC.SENDER_PARTY_ID", $buyerInformation["party_id"])
        ->equalTo("XDOC.RECEPIENT_PARTY_ID", $supplierInformation["party_id"])
        ->expression("desdoc.STATUS & ?", 8);
    $deljitShipmentCumulativeColumns = [
        "LastAsnShipmentCumulativeQuantity" => new Expression("MAX(deld.LastAsnShipmentCumulativeQuantity)"),
        "ItemSenderCode",
        "deld.ID"
    ];
    $deljitShipmentCumulative = $instanceS->get("XDOC_DELJIT_DETAIL", "deld", $deljitShipmentCumulativeColumns, $deljitShipmentCumulativeWhere, false, false, [
        ["name" => "XDOC", "on" => "deld.XDOC_ID = XDOC.ID", "columns" => [], "type" => Select::JOIN_INNER],
        ["name" => ["des" => "XDOC_DESADV"], "on" => "deld.LastDeliveredDesadvID = des.ID", "columns" => [], "type" => Select::JOIN_INNER],
        ["name" => ["desdoc" => "XDOC"], "on" => "des.XDOC_ID = desdoc.ID", "columns" => [], "type" => Select::JOIN_INNER]
    ], "deld.ItemSenderCode");
    foreach ($deljitShipmentCumulative as $deljitShipment) {
        $findRelation = $instanceT->get("v2_migration", "m", [], ["XDOC_DELJIT_DETAIL_ID" => $deljitShipment["ID"], false, false, [
            ["name" => ["ol" => "order_line"], "on" => "ol.order_line_id = m.order_line_id", "columns" => ["fk_product_id"], "type" => Select::JOIN_LEFT],
            ["name" => ["oc" => "order_consignee"], "on" => "ol.fk_order_consignee_id = oc.order_consignee_id", "columns" => ["fk_consignee_id"], "type" => Select::JOIN_LEFT],
        ]]);

        $instanceT->update("cumulative", [
            "current_dispetched" => $deljitShipment["LastAsnShipmentCumulativeQuantity"]
        ], [
            "fk_product_id" => $findRelation["fk_product_id"],
            "fk_party_id" => $supplierInformation["party_id"],
            "fk_consignee_id" => $findRelation["fk_consignee_id"]
        ]);
    }

    $deljitReceivedCumulativeWhere = new Where();
    $deljitReceivedCumulativeWhere->equalTo("XDOC.SENDER_PARTY_ID", $buyerInformation["party_id"])
        ->equalTo("XDOC.RECEPIENT_PARTY_ID", $supplierInformation["party_id"]);
    $deljitReceivedCumulativeColumns = [
        "LastReceivedCumulativeQuantity" => new Expression("MAX(deld.LastReceivedCumulativeQuantity)"),
        "ItemSenderCode"
    ];
    $deljitReceivedCumulative = $instanceS->get("XDOC_DELJIT_DETAIL", "deld", $deljitReceivedCumulativeColumns, $deljitReceivedCumulativeWhere, false, false, [
        ["name" => "XDOC", "on" => "deld.XDOC_ID = XDOC.ID", "columns" => [], "type" => Select::JOIN_INNER],
        ["name" => ["des" => "XDOC_DESADV"], "on" => "deld.LastDeliveredDesadvID = des.ID", "columns" => [], "type" => Select::JOIN_INNER],
        ["name" => ["desdoc" => "XDOC"], "on" => "des.XDOC_ID = desdoc.ID", "columns" => [], "type" => Select::JOIN_INNER]
    ], "deld.ItemSenderCode");
    foreach ($deljitShipmentCumulative as $deljitShipment) {
        $findRelation = $instanceT->get("v2_migration", "m", [], ["XDOC_DELJIT_DETAIL_ID" => $deljitShipment["ID"], false, false, [
            ["name" => ["ol" => "order_line"], "on" => "ol.order_line_id = m.order_line_id", "columns" => ["fk_product_id"], "type" => Select::JOIN_LEFT],
            ["name" => ["oc" => "order_consignee"], "on" => "ol.fk_order_consignee_id = oc.order_consignee_id", "columns" => ["fk_consignee_id"], "type" => Select::JOIN_LEFT],
        ]]);

        $instanceT->update("cumulative", [
            "current_acknowledged" => $deljitShipment["LastReceivedCumulativeQuantity"]
        ], [
            "fk_product_id" => $findRelation["fk_product_id"],
            "fk_party_id" => $supplierInformation["party_id"],
            "fk_consignee_id" => $findRelation["fk_consignee_id"]
        ]);
    }
}


/*
 select max(deld.LastAsnShipmentCumulativeQuantity),
            deld.ItemSenderCode
     from XDOC_DELJIT_DETAIL deld
              inner join XDOC on deld.XDOC_ID = XDOC.ID
              inner join XDOC_DESADV des on deld.LastDeliveredDesadvID = des.ID
              inner join XDOC desdoc on des.XDOC_ID = desdoc.ID
     where XDOC.SENDER_PARTY_ID = :senderId
       and XDOC.RECEPIENT_PARTY_ID = :recipientId
       and desdoc.STATUS & 8  group by deld.ItemSenderCode;


 select max(deld.LastReceivedCumulativeQuantity),
            deld.ItemSenderCode
     from XDOC_DELJIT_DETAIL deld
              inner join XDOC on deld.XDOC_ID = XDOC.ID
              inner join XDOC_DESADV des on deld.LastDeliveredDesadvID = des.ID
              inner join XDOC desdoc on des.XDOC_ID = desdoc.ID
     where XDOC.SENDER_PARTY_ID = :senderId
       and XDOC.RECEPIENT_PARTY_ID = :recipientId
       group by deld.ItemSenderCode;

 */
