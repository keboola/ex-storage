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

class TokenValidator
{
    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    public function validate(): string
    {
        $tokenInfo = $this->client->verifyToken();
        if (count($tokenInfo['bucketPermissions']) <> 1) {
            throw new UserException('The token must have read-only permissions to a single bucket only.');
        }
        $bucket = array_keys($tokenInfo['bucketPermissions'])[0];
        if ($tokenInfo['bucketPermissions'][$bucket] !== 'read') {
            throw new UserException(
                sprintf('The token must have read-only permissions to the bucket "%s".', $bucket)
            );
        }
        return $bucket;
    }
}
