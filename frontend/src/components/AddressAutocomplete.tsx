import { useEffect, useRef, useState, useCallback } from "react";

const GOOGLE_MAPS_API_KEY = import.meta.env.VITE_GOOGLE_MAPS_API_KEY as
  | string
  | undefined;

let googleScriptPromise: Promise<void> | null = null;

function loadGoogleMapsScript(): Promise<void> {
  if (window.google?.maps?.places) return Promise.resolve();
  if (googleScriptPromise) return googleScriptPromise;

  googleScriptPromise = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = `https://maps.googleapis.com/maps/api/js?key=${GOOGLE_MAPS_API_KEY}&libraries=places`;
    script.async = true;
    script.defer = true;
    script.onload = () => resolve();
    script.onerror = () => {
      googleScriptPromise = null;
      reject(new Error("Failed to load Google Maps script"));
    };
    document.head.appendChild(script);
  });

  return googleScriptPromise;
}

export interface PlaceResult {
  formattedAddress: string;
  stateProvince: string | null;
  country: string | null;
}

function extractAddressComponents(place: google.maps.places.PlaceResult): PlaceResult {
  const components = place.address_components || [];
  let stateProvince: string | null = null;
  let country: string | null = null;

  for (const c of components) {
    if (c.types.includes("administrative_area_level_1")) {
      stateProvince = c.short_name;
    }
    if (c.types.includes("country")) {
      country = c.short_name;
    }
  }

  return {
    formattedAddress: place.formatted_address || "",
    stateProvince,
    country,
  };
}

interface Props {
  value: string;
  onChange: (value: string) => void;
  onPlaceSelected?: (result: PlaceResult) => void;
  onSubmit?: () => void;
  placeholder?: string;
  className?: string;
}

export default function AddressAutocomplete({
  value,
  onChange,
  onPlaceSelected,
  onSubmit,
  placeholder = "Enter address (e.g., 123 Main St, Austin, TX 78701)",
  className,
}: Props) {
  const inputRef = useRef<HTMLInputElement>(null);
  const autocompleteRef = useRef<google.maps.places.Autocomplete | null>(null);
  const [apiAvailable, setApiAvailable] = useState(false);

  useEffect(() => {
    if (!GOOGLE_MAPS_API_KEY) return;

    loadGoogleMapsScript()
      .then(() => {
        setApiAvailable(true);

        if (!inputRef.current || autocompleteRef.current) return;

        const ac = new window.google.maps.places.Autocomplete(
          inputRef.current,
          {
            types: ["address"],
            componentRestrictions: { country: ["us", "ca"] },
            fields: ["formatted_address", "address_components"],
          }
        );

        ac.addListener("place_changed", () => {
          const place = ac.getPlace();
          if (place?.formatted_address) {
            const result = extractAddressComponents(place);
            onChange(result.formattedAddress);
            onPlaceSelected?.(result);
          }
        });

        autocompleteRef.current = ac;
      })
      .catch(() => {
        /* fall back to plain input */
      });

    return () => {
      if (autocompleteRef.current) {
        google.maps.event.clearInstanceListeners(autocompleteRef.current);
        autocompleteRef.current = null;
      }
    };
  }, []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key !== "Enter") return;

      const pacContainer = document.querySelector(".pac-container");
      const dropdownVisible =
        pacContainer &&
        window.getComputedStyle(pacContainer).display !== "none";

      if (dropdownVisible) return;

      onSubmit?.();
    },
    [onSubmit]
  );

  return (
    <input
      ref={inputRef}
      type="text"
      className={className}
      placeholder={
        apiAvailable
          ? "Start typing an address..."
          : placeholder
      }
      value={value}
      onChange={(e) => onChange(e.target.value)}
      onKeyDown={handleKeyDown}
    />
  );
}
