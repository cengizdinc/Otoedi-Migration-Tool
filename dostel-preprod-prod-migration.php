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
use Otoedi\Migration\Helper\Db;
use Map\Logger\EchoLogger;

$logger = new EchoLogger();
$dbConfig = json_decode(file_get_contents(__DIR__ . "/config/dostel.config.json"));
!empty($dbConfig) or die('Can not be found the configuration parameters!');
$sourceDb = new Db((array)$dbConfig->source);
$targetDb = new Db((array)$dbConfig->target);
$targetConnection = $targetDb->adapter->getDriver()->getConnection();
$targetConnection->beginTransaction();
$prList = ['585', '586'];
$supplierId = '60';
$shipments = $sourceDb->get('shipment', 's', [], ['fk_party_id' => $supplierId]);
$shipments = !isset($shipments[0]) ? [$shipments] : $shipments;
$shipmentXref = [];
foreach ($shipments as $shipment) {
    $oldShipment = $shipment["shipment_id"];
    unset($shipment["shipment_id"]);
    try {
        $shipmentId = $targetDb->create('shipment', $shipment);
        $shipmentXref[$oldShipment] = $shipmentId;
    } catch (InvalidQueryException $e) {
        $logger->error("Cannot create shipment, {$e->getMessage()}. Aborting.");
        $targetConnection->rollback();
        die();
    }
}
$products = $sourceDb->get('product', 'p', [], ['fk_supplier_id' => $supplierId]);
$products = !isset($products[0]) ? [$products] : $products;
$productXref = [];
$ppacXref = [];
foreach ($products as $product) {
    $oldProduct = $product["product_id"];
    unset($product["product_id"]);
    try {
        $productId = $targetDb->create('product', $product);
        $productXref[$oldProduct] = $productId;
    } catch (InvalidQueryException $e) {
        $logger->error("Cannot create product, {$e->getMessage()}. Aborting.");
        $targetConnection->rollback();
        die();
    }
    $ppackaging = $sourceDb->get('product_packaging', 'ppac', [], ['fk_product_id' => $oldProduct]);
    $ppackaging = (!empty($ppackaging) and !isset($ppackaging[0])) ? [$ppackaging] : $ppackaging;

    foreach ($ppackaging as $ppac) {
        $oldPpac = $ppac["product_packaging_id"];
        unset($ppac["product_packaging_id"]);
        $ppac["fk_product_id"] = $productXref[$ppac["fk_product_id"]];
        try {
            $ppacId = $targetDb->create('product_packaging', $ppac);
            $ppacXref[$oldPpac] = $ppacId;
        } catch (InvalidQueryException $e) {
            $logger->error("Cannot create product_packaging, {$e->getMessage()}. Aborting.");
            $targetConnection->rollback();
            die();
        }
    }
    $cumulative = $sourceDb->get('cumulative', 'c', [], ['fk_product_id' => $oldProduct]);
    $cumulative = (!empty($cumulative) and !isset($cumulative[0])) ? [$cumulative] : $cumulative;
    foreach ($cumulative as $c) {
        unset($c['current_variance']);
        unset($c['cumulative_id']);
        $c["fk_product_id"] = $productXref[$c["fk_product_id"]];
        try {
            $targetDb->update('cumulative', $c, ["fk_product_id" => $c["fk_product_id"], "fk_party_id" => $c["fk_party_id"], "fk_consignee_id" => $c["fk_consignee_id"]]);
        } catch (InvalidQueryException $e) {
            $logger->error("Cannot create cumulative, {$e->getMessage()}. Aborting.");
            $targetConnection->rollback();
            die();
        }
    }
}
$documents = $sourceDb->get('document', 'd', [], ['fk_pr_id' => $prList], false, ['fk_dt_id']);
$documents = !isset($documents[0]) ? [$documents] : $documents;
$documentXref = [];
$olXref = [];
$orderXref = [];
$orderConsigneeXref = [];
$despatchXref = [];
$dpXref = [];
foreach ($documents as $document) {
    $oldDocument = $document["document_id"];
    unset($document["document_id"]);
    unset($document["is_valid"]);
    try {
        $documentId = $targetDb->create('document', $document);
        $documentXref[$oldDocument] = $documentId;
    } catch (InvalidQueryException $e) {
        $logger->error("Cannot create document, {$e->getMessage()}. Aborting.");
        $targetConnection->rollback();
        die();
    }
    switch ($document["fk_dt_id"]) {
        case "1":
            $orders = $sourceDb->get('order', 'o', [], ['fk_document_id' => $oldDocument]);
            $orders = (!empty($orders) and !isset($orders[0])) ? [$orders] : $orders;
            foreach ($orders as $order) {
                $oldOrder = $order["order_id"];
                unset($order["order_id"]);
                $order["fk_document_id"] = $documentXref[$order["fk_document_id"]];
                try {
                    $orderId = $targetDb->create('order', $order);
                    $orderXref[$oldOrder] = $orderId;
                } catch (InvalidQueryException $e) {
                    $logger->error("Cannot create order, {$e->getMessage()}. Aborting.");
                    $targetConnection->rollback();
                    die();
                }

                $orderConsignee = $sourceDb->get('order_consignee', 'oc', [], ['fk_order_id' => $oldOrder]);
                $orderConsignee = !isset($orderConsignee[0]) ? [$orderConsignee] : $orderConsignee;
                foreach ($orderConsignee as $oc) {
                    $oldConsignee = $oc["order_consignee_id"];
                    unset($oc["order_consignee_id"]);
                    $oc["fk_order_id"] = $orderXref[$oc["fk_order_id"]];
                    try {
                        $consigneeId = $targetDb->create('order_consignee', $oc);
                        $orderConsigneeXref[$oldConsignee] = $consigneeId;
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create order_consignee, {$e->getMessage()}; order: {$order["order_id"]}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }

                $orderLines = $sourceDb->get('order_line', 'ol', [], ['fk_order_id' => $oldOrder]);
                $orderLines = !isset($orderLines[0]) ? [$orderLines] : $orderLines;

                foreach ($orderLines as $ol) {
                    $oldOl = $ol["order_line_id"];
                    unset($ol["order_line_id"]);
                    unset($ol["is_confirming_completed"]);
                    unset($ol["is_packaging_completed"]);
                    unset($ol["is_shipping_completed"]);
                    unset($ol["is_invoicing_completed"]);

                    $ol["fk_order_id"] = $orderXref[$ol["fk_order_id"]];
                    $ol["fk_order_consignee_id"] = $orderConsigneeXref[$ol["fk_order_consignee_id"]];
                    $ol["fk_product_id"] = $productXref[$ol["fk_product_id"]];
                    try {
                        $orderLineId = $targetDb->create('order_line', $ol);
                        $olXref[$oldOl] = $orderLineId;
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create order_line, {$e->getMessage()}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                    try {
                        $orderLineLogs = $sourceDb->get('order_line_log', 'oll', [], ['fk_ol_id' => $orderLineId]);
                        $orderLineLogs = (!empty($orderLineLogs) and !isset($orderLineLogs[0])) ? [$orderLineLogs] : $orderLineLogs;
                        foreach ($orderLineLogs as $oll) {
                            unset($oll["log_id"]);
                            $oll["fk_order_id"] = $orderXref[$oll["fk_order_id"]];
                            $oll["fk_ol_id"] = $orderLineId;
                            $targetDb->create('order_line_log', $oll);
                        }
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create order_line_log, {$e->getMessage()}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }
            }
            break;
        case "2":
            $despatches = $sourceDb->get('despatch', 'd', [], ['fk_document_id' => $oldDocument]);
            $despatches = (!empty($despatches) and !isset($despatches[0])) ? [$despatches] : $despatches;
            foreach ($despatches as $despatch) {
                $oldDespatch = $despatch["despatch_id"];
                unset($despatch["despatch_id"]);
                $despatch["fk_document_id"] = $documentXref[$despatch["fk_document_id"]];
                if (!empty($despatch["fk_shipment_id"])) {
                    $despatch["fk_shipment_id"] = $shipmentXref[$despatch["fk_shipment_id"]];
                }
                try {
                    $despatchId = $targetDb->create('despatch', $despatch);
                    $despatchXref[$oldDespatch] = $despatchId;
                } catch (InvalidQueryException $e) {
                    $logger->error("Cannot create despatch, {$e->getMessage()}. Aborting.");
                    $targetConnection->rollback();
                    die();
                }

                $despatchPackages = $sourceDb->get('despatch_package', 'dp', [], ['fk_despatch_id' => $oldDespatch]);
                $despatchPackages = (!empty($despatchPackages) and !isset($despatchPackages[0])) ? [$despatchPackages] : $despatchPackages;
                foreach ($despatchPackages as $dp) {
                    $oldDp = $dp["despatch_package_id"];
                    unset($dp["despatch_package_id"]);
                    $dp["fk_despatch_id"] = $despatchXref[$dp["fk_despatch_id"]];
                    $dp["fk_product_packaging_id"] = @$ppacXref[$dp["fk_product_packaging_id"]];
                    $dp["fk_order_line_id"] = @$olXref[$dp["fk_order_line_id"]];
                    if (!empty($dp["fk_despatch_package_id"])) {
                        $dp["fk_despatch_package_id"] = @$dpXref[$dp["fk_despatch_package_id"]];
                    }
                    try {
                        $dpId = $targetDb->create('despatch_package', $dp);
                        $dpXref[$oldDp] = $dpId;
                    } catch (InvalidQueryException $e) {
                        print_r($dp);
                        $logger->error("Cannot create despatch_package, {$e->getMessage()}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }
                $despatchProduct = $sourceDb->get('despatch_product', 'dpr', [], ['fk_despatch_id' => $oldDespatch]);
                $despatchProduct = !isset($despatchProduct[0]) ? [$despatchProduct] : $despatchProduct;
                foreach ($despatchProduct as $dpr) {
                    unset($dpr["despatch_product_id"]);
                    $dpr["fk_despatch_id"] = $despatchXref[$dpr["fk_despatch_id"]];
                    $dpr["fk_order_line_id"] = $olXref[$dpr["fk_order_line_id"]];
                    $dpr["fk_product_id"] = $productXref[$dpr["fk_product_id"]];
                    try {
                        $targetDb->create('despatch_product', $dpr);
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create despatch_product, {$e->getMessage()}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }
                $integrationQueue = $sourceDb->get('integration_queue', 'dpr', [], ['fk_document_id' => $oldDocument]);
                $integrationQueue = !isset($integrationQueue[0]) ? [$integrationQueue] : $integrationQueue;
                foreach ($integrationQueue as $iq) {
                    unset($iq["integration_queue_id"]);
                    $iq["fk_document_id"] = $documentXref[$iq["fk_document_id"]];
                    try {
                        $targetDb->create('integration_queue', $iq);
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create iq, {$e->getMessage()}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }
            }
    }
}
$targetConnection->commit();
