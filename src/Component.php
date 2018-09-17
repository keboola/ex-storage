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
            $tokenValidator = new TokenValidator($client);
            $bucket = $tokenValidator->validate();
            if ($config->getAction() === 'run') {
                $this->extract($client, $config, $bucket);
            } elseif ($config->getAction() === 'list') {
                echo \GuzzleHttp\json_encode([
                    'tables' => $this->listTables($client, $bucket),
                ]);
            } else {
                throw new UserException(sprintf('Unknown action "%s"', $config->getAction()));
            }
        } catch (ClientException $e) {
            throw new UserException($e->getMessage());
        }
    }

    private function extract(Client $client, Config $config, string $bucket) : void
    {
        if (empty($config->getTableName())) {
            throw new UserException("The tableName parameter must be provided.");
        }
        $this->getLogger()->info(sprintf('Processing table "%s".', $config->getTableName()));
        $exporter = new TableExporter($client);
        $tableId = $bucket . '.' . $config->getTableName();
        if (!empty($config->getChangedSince())) {
            $options['changedSince'] = $config->getChangedSince();
        } else {
            $options = [];
        }
        $exporter->exportTable(
            $tableId,
            $this->getDataDir() . '/out/tables/' . $config->getTableName() . '.csv',
            $options
        );
        $manifestOptions = new OutTableManifestOptions();
        if ($config->extractMetadata()) {
            $tableInfo = $client->getTable($tableId);
            $metadata = new Metadata($client);
            $columnMetadata = [];
            foreach ($tableInfo['columns'] as $column) {
                $colMetadata = $this->filterMetadata(
                    $metadata->listColumnMetadata($tableId . '.' . $column)
                );
                if ($colMetadata) {
                    $columnMetadata[$column] = $colMetadata;
                }
            }
            $tableMetadata = $this->filterMetadata($metadata->listTableMetadata($tableId));
            if ($columnMetadata) {
                $manifestOptions->setColumnMetadata($columnMetadata);
            }
            if ($tableMetadata) {
                $manifestOptions->setMetadata($tableMetadata);
            }
        }
        $this->getManifestManager()->writeTableManifest($config->getTableName() . '.csv', $manifestOptions);
        $this->getLogger()->info(sprintf('Table "%s" processed.', $config->getTableName()));
    }

    private function listTables(Client $client, string $bucket) : array
    {
        $tables = $client->listTables($bucket);
        array_walk($tables, function (&$value) : void {
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
