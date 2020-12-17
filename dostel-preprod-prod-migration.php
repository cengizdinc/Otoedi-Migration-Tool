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

$supplierId = '60';

$products =  $sourceDb->get('product', 'p', ['fk_supplier_id' => $supplierId]);
foreach ($products as $product) {
    try {
        $targetDb->create('product', $product);
    } catch (InvalidQueryException $e) {
        $logger->error("Cannot create product, {$e->getMessage()}; product: {$product["product_id"]}. Aborting.");
        $targetConnection->rollback();
        die();
    }
    $ppackaging = $sourceDb->get('product_packaging', 'ppac', ['fk_product_id' => $product["product_id"]]);
    foreach ($ppackaging as $ppac) {
        try {
            $targetDb->create('product_packaging', $ppac);
        } catch (InvalidQueryException $e) {
            $logger->error("Cannot create product_packaging, {$e->getMessage()}; product: {$product["product_id"]}. Aborting.");
            $targetConnection->rollback();
            die();
        }
    }

    $cumulative = $sourceDb->get('cumulative', 'c', ['fk_product_id' => $product["product_id"]]);
    foreach ($cumulative as $c) {
        try {
            $targetDb->create('cumulative', $c);
        } catch (InvalidQueryException $e) {
            $logger->error("Cannot create cumulative, {$e->getMessage()}; product: {$product["product_id"]}. Aborting.");
            $targetConnection->rollback();
            die();
        }
    }
}

$prList = ['585', '586'];
$documents = $sourceDb->get('document', 'd', ['fk_pr_id' => $prList]);
foreach ($documents as $document) {
    $targetConnection->beginTransaction();
    try {
        $targetDb->create('document', $document);
    } catch (InvalidQueryException $e) {
        $logger->error("Cannot create document, {$e->getMessage()}; document_id: {$document["document_id"]}. Aborting.");
        $targetConnection->rollback();
        die();
    }

    switch ($document["fk_dt_id"]) {
        case "1":
            $orders = $sourceDb->get('order', 'o', ['fk_document_id' => $document['document_id']]);
            foreach ($orders as $order) {
                try {
                    $targetDb->create('order', $order);
                } catch (InvalidQueryException $e) {
                    $logger->error("Cannot create order, {$e->getMessage()}; order_id: {$order["order_id"]}. Aborting.");
                    $targetConnection->rollback();
                    die();
                }

                $orderConsignee = $sourceDb->get('order_consignee', 'oc', ['fk_order_id' => $order['fk_order_id']]);
                foreach ($orderConsignee as $oc) {
                    try {
                        $targetDb->create('order_consignee', $oc);
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create order_consignee, {$e->getMessage()}; order: {$order["order_id"]}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }

                $orderLines = $sourceDb->get('order_line', 'ol', ['fk_order_id' => $order['fk_order_id']]);
                foreach ($orderLines as $ol) {
                    try {
                        $targetDb->create('order_line', $ol);
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create order_line, {$e->getMessage()}; order_id: {$order["order_id"]}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }

                    try {
                        $orderLineLogs = $sourceDb->get('order_line_log', 'oll', ['fk_ol_id' => $ol['order_line_id']]);
                        foreach ($orderLineLogs as $oll) {
                            $targetDb->create('order_line_log', $oll);
                        }
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create order_line_log, {$e->getMessage()}; order_id: {$order["order_id"]}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }
            }
            break;
        case "2":
            $despatches = $sourceDb->get('despatch', 'd', ['fk_document_id' => $document['document_id']]);
            foreach ($despatches as $despatch) {
                try {
                    $targetDb->create('despatch', $despatch);
                } catch (InvalidQueryException $e) {
                    $logger->error("Cannot create despatch, {$e->getMessage()}; despatch: {$despatch["despatch_id"]}. Aborting.");
                    $targetConnection->rollback();
                    die();
                }

                $despatchPackages = $sourceDb->get('despatch_package', 'dp', ['fk_despatch_id' => $despatch['despatch_id']]);
                foreach ($despatchPackages as $dp) {
                    try {
                        $targetDb->create('despatch_package', $dp);
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create despatch_package, {$e->getMessage()}; despatch: {$despatch["despatch_id"]}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }

                $despatchProduct = $sourceDb->get('despatch_product', 'dpr', ['fk_despatch_id' => $despatch['despatch_id']]);
                foreach ($despatchProduct as $dpr) {
                    try {
                        $targetDb->create('despatch_product', $dpr);
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create despatch_product, {$e->getMessage()}; despatch: {$despatch["despatch_id"]}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }

                $integrationQueue = $sourceDb->get('integration_queue', 'dpr', ['fk_document_id' => $document['document_id']]);
                foreach ($integrationQueue as $iq) {
                    try {
                        $targetDb->create('integration_queue', $iq);
                    } catch (InvalidQueryException $e) {
                        $logger->error("Cannot create iq, {$e->getMessage()}; fk_document_id: {$document['document_id']}. Aborting.");
                        $targetConnection->rollback();
                        die();
                    }
                }
            }
    }
}
