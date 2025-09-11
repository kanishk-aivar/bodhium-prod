import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';
import { getToken } from 'next-auth/jwt';

export async function middleware(req: NextRequest) {
  const url = req.nextUrl;
  const { pathname } = url;
  
  // Exclude authentication, login routes, and health check endpoints
  if (pathname.startsWith('/api/auth') || 
      pathname.startsWith('/login') ||
      pathname === '/health' ||
      pathname === '/api/health' ||
      pathname.startsWith('/_next') ||
      pathname === '/favicon.ico') {
    return NextResponse.next();
  }

  // Extract token
  const token = await getToken({ req, secret: process.env.NEXTAUTH_SECRET });

  // API-specific logic
  if (pathname.startsWith('/api')) {
    if (!token) {
      return NextResponse.json({ message: 'Unauthorized' }, { status: 401 });
    }
    return NextResponse.next();
  }

  // Frontend-specific logic - protect main app routes
  if (pathname === '/' || 
      pathname.startsWith('/products') || 
      pathname.startsWith('/queries') || 
      pathname.startsWith('/results') || 
      pathname.startsWith('/adhoc')) {
    if (!token) {
      const loginUrl = new URL('/login', url.origin);
      loginUrl.searchParams.set('redirect', pathname);
      return NextResponse.redirect(loginUrl);
    }
    return NextResponse.next();
  }

  // Allow all other requests (static assets, public files, etc.)
  return NextResponse.next();
}

export const config = {
  matcher: [
    /*
     * Apply middleware to all routes except static assets
     * The authorization logic in the middleware function handles what's allowed/blocked
     */
    "/((?!_next/static|_next/image|favicon.ico).*)",
  ],
}
