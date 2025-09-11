import { NextAuthOptions } from "next-auth"
import GoogleProvider from "next-auth/providers/google"
import { DynamoDBClient } from "@aws-sdk/client-dynamodb"
import { DynamoDBDocumentClient, GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb"

// Configure AWS DynamoDB client
const client = new DynamoDBClient({
  region: process.env.AWS_REGION || "us-east-1",
})

const docClient = DynamoDBDocumentClient.from(client)

export interface OAuthUser {
  id: string
  email: string
  name?: string
  image?: string
  createdAt: string
  updatedAt: string
}

// Allowed email domains
const ALLOWED_DOMAINS = ['aivar.tech', 'bodhiumlabs.com']

// Function to validate email domain
function isEmailDomainAllowed(email: string): boolean {
  const domain = email.split('@')[1]
  return ALLOWED_DOMAINS.includes(domain)
}

// Function to find or create user by email
async function findOrCreateUser(email: string, name?: string, image?: string): Promise<OAuthUser | null> {
  try {
    // First, try to find existing user
    const getCommand = new GetCommand({
      TableName: "oauth_users",
      Key: { email }
    })

    const response = await docClient.send(getCommand)
    
    if (response.Item) {
      // Update last login time
      const updateCommand = new PutCommand({
        TableName: "oauth_users",
        Item: {
          ...response.Item,
          updatedAt: new Date().toISOString()
        }
      })
      await docClient.send(updateCommand)
      return response.Item as OAuthUser
    }

    // Create new user if not found
    const newUser: OAuthUser = {
      id: email, // Using email as ID for simplicity
      email,
      name: name || email,
      image,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    }

    const putCommand = new PutCommand({
      TableName: "oauth_users",
      Item: newUser
    })

    await docClient.send(putCommand)
    return newUser
  } catch (error) {
    console.error("Error finding/creating user:", error)
    return null
  }
}

export const authOptions: NextAuthOptions = {
  providers: [
    GoogleProvider({
      clientId: process.env.GOOGLE_CLIENT_ID!,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET!,
    })
  ],
  session: {
    strategy: "jwt",
    maxAge: 24 * 60 * 60, // 24 hours
  },
  jwt: {
    maxAge: 24 * 60 * 60, // 24 hours
  },
  pages: {
    signIn: "/login",
  },
  callbacks: {
    async signIn({ user, account, profile }) {
      // Only allow Google OAuth
      if (account?.provider !== "google") {
        return false
      }

      // Check if email domain is allowed
      if (!user.email || !isEmailDomainAllowed(user.email)) {
        console.log(`Access denied for email: ${user.email}`)
        return false
      }

      return true
    },
    async jwt({ token, user, account }) {
      if (user && account?.provider === "google") {
        // Find or create user in DynamoDB
        const dbUser = await findOrCreateUser(
          user.email!,
          user.name,
          user.image
        )
        
        if (dbUser) {
          token.id = dbUser.id
          token.email = dbUser.email
          token.name = dbUser.name
          token.image = dbUser.image
        }
      }
      return token
    },
    async session({ session, token }) {
      if (token) {
        session.user.id = token.id as string
        session.user.email = token.email as string
        session.user.name = token.name as string
        session.user.image = token.image as string
      }
      return session
    },
  },
}
