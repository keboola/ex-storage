<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor;

use Keboola\Component\BaseComponent;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;
use Keboola\Component\UserException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\TableExporter;

class Component extends BaseComponent
{
    public function run(): void
    {
        try {
            /** @var Config $config */
            $config = $this->getConfig();
            $client = new Client(['token' => $config->getToken(), 'url' => $config->getUrl()]);
            $tokenInfo = $client->verifyToken();
            if (count($tokenInfo['bucketPermissions']) > 1) {
                throw new UserException('The token has too broad permissions.');
            }
            $bucket = array_keys($tokenInfo['bucketPermissions'])[0];
            if ($tokenInfo['bucketPermissions'][$bucket] !== 'read') {
                throw new UserException('The token does not have only read permissions to the bucket ' . $bucket);
            }
            if ($config->getAction() === 'run') {
                $this->extract($client, $config, $bucket);
            } elseif ($config->getAction() === 'list') {
                echo \GuzzleHttp\json_encode([
                    'tables' => $this->listTables($client, $bucket),
                ]);
            } else {
                throw new UserException("Unknown action " . $config->getAction());
            }
        } catch (ClientException $e) {
            throw new UserException($e->getMessage());
        }
    }

    private function extract(Client $client, Config $config, string $bucket) : void
    {
        $tables = $config->getTables();
        if (empty($tables)) {
            $tables = $this->listTables($client, $bucket);
        }
        foreach ($tables as $tableName) {
            $this->getLogger()->info('Processing table ' . $tableName);
            $exporter = new TableExporter($client);
            $tableId = $bucket . '.' . $tableName;
            $exporter->exportTable($tableId, $this->getDataDir() . '/out/tables/' . $tableName . '.csv', []);
            $tableInfo = $client->getTable($tableId);
            $metadata = new Metadata($client);
            $columnMetadata = [];
            foreach ($tableInfo['columns'] as $column) {
                $columnMetadata[$column] = $this->filterMetadata(
                    $metadata->listColumnMetadata($tableId . '.' . $column)
                );
            }
            $tableMetadata = $this->filterMetadata($metadata->listTableMetadata($tableId));
            $options = new OutTableManifestOptions();
            $options->setColumnMetadata($columnMetadata);
            $options->setMetadata($tableMetadata);
            $this->getManifestManager()->writeTableManifest($tableName . '.csv', $options);
            $this->getLogger()->info('Table ' . $tableName . ' processed.');
        }
    }

    private function listTables(Client $client, string $bucket) : array
    {
        $tables = $client->listTables($bucket);
        array_walk($tables, function (&$value) {
            $value = $value['name'];
        });
        return $tables;
    }

    private function filterMetadata(array $metadata) : array
    {
        $result = [];
        foreach ($metadata as $item) {
            $result[] = array_filter(
                $item,
                function ($key) {
                    return in_array($key, ['key', 'value']);
                },
                ARRAY_FILTER_USE_KEY
            );
        }
        return $result;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
