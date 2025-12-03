import React from "react"
import ReactDOM from "react-dom/client"
import App from "./App"
import "./index.css"  // where Tailwind/global styles will go (optional but common)

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)
