import { Routes, Route, NavLink } from "react-router-dom";
import { useAuth } from "./auth/AuthContext";
import LookupPage from "./pages/LookupPage";
import UtilitiesPage from "./pages/UtilitiesPage";
import UtilityDetailPage from "./pages/UtilityDetailPage";
import TariffDetailPage from "./pages/TariffDetailPage";
import TariffBrowserPage from "./pages/TariffBrowserPage";
import MonitoringPage from "./pages/MonitoringPage";
import LoginPage from "./pages/LoginPage";

export default function App() {
  const { loading, error, me, logout } = useAuth();

  if (loading) {
    return (
      <div className="auth-loading">
        <p>Loading…</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="auth-loading">
        <p>Could not reach the API.</p>
        <p className="auth-error-detail">{error}</p>
      </div>
    );
  }

  const needLogin = me?.auth_enabled && !me.authenticated;

  if (needLogin) {
    return <LoginPage />;
  }

  const signedInLabel =
    me?.auth_enabled && me.authenticated && me.email ? me.email : null;

  return (
    <div className="app">
      <header className="header">
        <div className="header-inner">
          <h1 className="logo">Utility Tariff Finder</h1>
          <nav className="nav">
            <NavLink to="/" end>Address Lookup</NavLink>
            <NavLink to="/utilities">Utilities</NavLink>
            <NavLink to="/tariffs">Tariffs</NavLink>
            <NavLink to="/admin/monitoring">Monitoring</NavLink>
            {signedInLabel ? (
              <span className="nav-user">
                <span className="nav-user-email">{signedInLabel}</span>
                <button type="button" className="btn-link" onClick={() => void logout()}>
                  Sign out
                </button>
              </span>
            ) : null}
          </nav>
        </div>
      </header>
      <main className="main">
        <Routes>
          <Route path="/" element={<LookupPage />} />
          <Route path="/utilities" element={<UtilitiesPage />} />
          <Route path="/utilities/:id" element={<UtilityDetailPage />} />
          <Route path="/tariffs" element={<TariffBrowserPage />} />
          <Route path="/tariffs/:id" element={<TariffDetailPage />} />
          <Route path="/admin/monitoring" element={<MonitoringPage />} />
        </Routes>
      </main>
    </div>
  );
}
