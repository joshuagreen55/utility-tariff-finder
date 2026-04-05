export default function LoginPage() {
  const loginUrl = `${window.location.origin}/api/auth/google/login`;

  return (
    <div className="login-gate">
      <div className="login-card">
        <h1 className="login-title">Utility Tariff Finder</h1>
        <p className="login-copy">Sign in with your <strong>@getmysa.com</strong> Google account.</p>
        <a className="btn btn-primary login-btn" href={loginUrl}>
          Continue with Google
        </a>
      </div>
    </div>
  );
}
