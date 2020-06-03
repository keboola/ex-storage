<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor\Tests;

use Keboola\Component\UserException;
use Keboola\StorageApi\Client;
use Keboola\StorageExtractor\Authorization;
use PHPUnit\Framework\TestCase;

class AuthorizationTest extends TestCase
{
    private function getClientMock(array $verifyTokenData): Client
    {
        $client = self::getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['verifyToken'])
            ->getMock();
        $client->method('verifyToken')
            ->willReturn($verifyTokenData);
        /** @var Client $client */
        return $client;
    }

    public function testValidToken(): void
    {
        $mockClientResponse = [
            'bucketPermissions' => [
                'in.some-bucket' => 'read',
            ],
            'owner' => [
                'id' => 1234,
                'name' => 'testProjectName',
            ],
        ];
        $client = $this->getClientMock($mockClientResponse);
        $authorization = new Authorization($client);
        self::assertSame('in.some-bucket', $authorization->getAuthorizedBucket());
        self::assertSame(1234, $authorization->getAuthorizedProjectId());
        self::assertSame('testProjectName', $authorization->getAuthorizedProjectName());
    }

    /**
     * @dataProvider invalidTokenPermissionsProvider
     * @param array $permissions
     * @param string $error
     */
    public function testInvalidToken(array $permissions, string $error): void
    {
        $client = $this->getClientMock($permissions);
        self::expectExceptionMessage($error);
        self::expectException(UserException::class);
        new Authorization($client);
    }

    public function invalidTokenPermissionsProvider(): array
    {
        return [
            [
                ['bucketPermissions' => ['in.some-bucket' => 'manage']],
                'The token must have read-only permissions to the bucket "in.some-bucket".',
            ],
            [
                ['bucketPermissions' => ['in.some-bucket' => 'read', 'out.another-bucket' => 'read']],
                'The token must have read-only permissions to a single bucket only.',
            ],
            [
                ['bucketPermissions' => []],
                'The token must have read-only permissions to a single bucket only.',
            ],
        ];
    }
}
