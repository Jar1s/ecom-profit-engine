import { Route, Routes } from 'react-router-dom'
import { Layout } from './components/Layout'
import { Accounting } from './pages/Accounting'
import { Costs } from './pages/Costs'
import { Daily } from './pages/Daily'
import { Dashboard } from './pages/Dashboard'
import { Jobs } from './pages/Jobs'
import { Marketing } from './pages/Marketing'
import { Orders } from './pages/Orders'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="orders" element={<Orders />} />
        <Route path="daily" element={<Daily />} />
        <Route path="marketing" element={<Marketing />} />
        <Route path="accounting" element={<Accounting />} />
        <Route path="costs" element={<Costs />} />
        <Route path="jobs" element={<Jobs />} />
      </Route>
    </Routes>
  )
}
