export default function HealthPage() {
  return (
    <div className="min-h-screen flex items-center justify-center">
      <div className="text-center">
        <h1 className="text-2xl font-bold text-green-600">Healthy</h1>
        <p className="text-gray-600">Service is running normally</p>
        <p className="text-sm text-gray-400 mt-2">
          {new Date().toISOString()}
        </p>
      </div>
    </div>
  )
}
