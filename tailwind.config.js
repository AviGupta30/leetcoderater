export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        slate: {
          950: '#020617',
          900: '#0f172a',
          800: '#1e293b',
          700: '#334155',
        },
        accent: {
          orange: '#f97316',
          emerald: '#10b981',
          rose: '#f43f5e',
          sky: '#38bdf8',
        }
      },
      animation: {
        'sheet-in': 'sheet-in 0.3s ease-out forwards',
        'fade-in': 'fade-in 0.2s ease-out forwards',
      },
      keyframes: {
        'sheet-in': {
          '0%': { transform: 'translateX(100%)' },
          '100%': { transform: 'translateX(0)' },
        },
        'fade-in': {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        }
      }
    },
  },
  plugins: [],
}
