const nextConfig = {
  env: {
    AWS_REGION: process.env.AWS_REGION,
    // AWS credentials removed - using IAM Task Role instead
    RDS_HOST: process.env.RDS_HOST,
    RDS_PORT: process.env.RDS_PORT,
    RDS_DATABASE: process.env.RDS_DATABASE,
    RDS_USERNAME: process.env.RDS_USERNAME,
    RDS_PASSWORD: process.env.RDS_PASSWORD,
    LAMBDA_WEBSCRAPPER_ARN: process.env.LAMBDA_WEBSCRAPPER_ARN,
    LAMBDA_QUERY_GENERATOR_ARN: process.env.LAMBDA_QUERY_GENERATOR_ARN,
    LAMBDA_LLM_ORCHESTRATOR_ARN: process.env.LAMBDA_LLM_ORCHESTRATOR_ARN,
  },
  eslint: {
    ignoreDuringBuilds: true,
  },
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    domains: ['placeholder.svg'],
    remotePatterns: [
      {
        protocol: 'https',
        hostname: '**',
      },
    ],
    unoptimized: true,
  },
  // Enable standalone output for Docker
  output: 'standalone',
}

export default nextConfig
