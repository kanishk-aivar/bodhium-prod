import { NextResponse } from 'next/server'

export async function GET() {
  return NextResponse.json(
    { 
      status: 'healthy', 
      timestamp: new Date().toISOString(),
      service: 'bodhium-workflow'
    },
    { status: 200 }
  )
}

export async function HEAD() {
  return new NextResponse(null, { status: 200 })
}
