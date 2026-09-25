from PIL import Image, ImageDraw

def create_vault_icon(output_path="vault.ico"):
    size = 256
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Outer rounded rectangle (vibrant gradient-like background)
    padding = 16
    r = 48
    box = [padding, padding, size - padding, size - padding]

    # Draw rounded background with indigo-to-purple gradient simulation
    draw.rounded_rectangle(box, radius=r, fill=(24, 30, 54, 255), outline=(99, 102, 241, 255), width=6)

    # Inner soft glow
    draw.rounded_rectangle([padding + 6, padding + 6, size - padding - 6, size - padding - 6], radius=r - 6, outline=(129, 140, 248, 80), width=4)

    # Draw isometric 3D storage cube / vault safe
    # Center: (128, 128)
    cx, cy = 128, 128
    s = 60  # scale

    # Top face (isometric)
    top_face = [
        (cx, cy - s),
        (cx + s * 1.0, cy - s * 0.45),
        (cx, cy + s * 0.1),
        (cx - s * 1.0, cy - s * 0.45)
    ]
    draw.polygon(top_face, fill=(99, 102, 241, 255), outline=(165, 180, 252, 255))

    # Left face
    left_face = [
        (cx - s * 1.0, cy - s * 0.45),
        (cx, cy + s * 0.1),
        (cx, cy + s * 1.1),
        (cx - s * 1.0, cy + s * 0.55)
    ]
    draw.polygon(left_face, fill=(67, 56, 202, 255), outline=(99, 102, 241, 255))

    # Right face
    right_face = [
        (cx, cy + s * 0.1),
        (cx + s * 1.0, cy - s * 0.45),
        (cx + s * 1.0, cy + s * 0.55),
        (cx, cy + s * 1.1)
    ]
    draw.polygon(right_face, fill=(49, 46, 129, 255), outline=(79, 70, 229, 255))

    # Center Vault core glow circle
    core_r = 16
    draw.ellipse([cx - core_r, cy + s * 0.1 - core_r, cx + core_r, cy + s * 0.1 + core_r], fill=(56, 189, 248, 255), outline=(224, 242, 254, 255), width=2)

    # Save as .ico with multi-resolution support
    icon_sizes = [(256, 256), (128, 128), (64, 64), (48, 48), (32, 32), (16, 16)]
    img.save(output_path, format="ICO", sizes=icon_sizes)
    print(f"[+] Saved high-resolution Windows icon to: {output_path}")

if __name__ == "__main__":
    create_vault_icon()
