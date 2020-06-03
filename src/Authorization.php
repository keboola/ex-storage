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

class Authorization
{
    /**
     * @var string
     */
    private $authorizedBucket;

    /**
     * @var string
     */
    private $authorizedProjectName;

    /**
     * @var int
     */
    private $authorizedProjectId;

    public function __construct(Client $client)
    {
        $tokenInfo = $client->verifyToken();
        $this->validateNumberOfBuckets($tokenInfo);
        $bucket = (string) array_keys($tokenInfo['bucketPermissions'])[0];
        $this->validateBucketPermissions($tokenInfo, $bucket);
        $this->authorizedBucket = $bucket;
        $this->authorizedProjectName = $tokenInfo['owner']['name'];
        $this->authorizedProjectId = $tokenInfo['owner']['id'];
    }

    private function validateNumberOfBuckets(array $tokenInfo): void
    {
        if (count($tokenInfo['bucketPermissions']) !== 1) {
            throw new UserException('The token must have read-only permissions to a single bucket only.');
        }
    }

    private function validateBucketPermissions(array $tokenInfo, string $bucket): void
    {
        if ($tokenInfo['bucketPermissions'][$bucket] !== 'read') {
            throw new UserException(
                sprintf('The token must have read-only permissions to the bucket "%s".', $bucket)
            );
        }
    }

    public function getAuthorizedBucket(): string
    {
        return $this->authorizedBucket;
    }

    public function getAuthorizedProjectName(): string
    {
        return $this->authorizedProjectName;
    }

    public function getAuthorizedProjectId(): int
    {
        return $this->authorizedProjectId;
    }
}
