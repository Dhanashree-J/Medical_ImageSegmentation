#!/usr/bin/env python3
import sys
import cv2
import numpy as np
import torch
#from segment_anything import sam_model_registry, SamPredictor
from mobile_sam import sam_model_registry, SamPredictor
# =========================
# 1. INPUT ARGUMENTS
# =========================
if len(sys.argv) < 3:
print("Usage: python sam_wrapper.py <input_image> <output_image>")
sys.exit(1)
input_path = sys.argv[1]
output_path = sys.argv[2]
# =========================
# 2. MODEL SETUP
# =========================
MODEL_TYPE="vit_t"
CHECKPOINT= "mobile_sam.pt"
device = "cuda" if torch.cuda.is_available() else "cpu"
sam = sam_model_registry[MODEL_TYPE](checkpoint=CHECKPOINT)
sam.to(device)
predictor = SamPredictor(sam)
# =========================
# 3. LOAD IMAGE
# =========================
# Read the raw bytes directly from the pipe
# Convert the bytes into an OpenCV image format
image_bgr = cv2.imread(input_path)
if image_bgr is None:
print("FAILED: OpenCV could not decode image bytes")
sys.exit(0)
image_bgr=cv2.resize(image_bgr, (480,480))
image_rgb = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2RGB)
h, w = image_rgb.shape[:2]
predictor.set_image(image_rgb)
# =========================
# 4. BETTER PROMPTS (Foreground + Background + Box)
# =========================
# Foreground (center region - likely lesion)
fg_points = np.array([
[w//2, h//2],
[w//2 + 20, h//2],
[w//2, h//2 + 20]
])
# Background (corners)
bg_points = np.array([
[10, 10],
[w-10, 10],
[10, h-10],
[w-10, h-10]
])
input_points = np.vstack((fg_points, bg_points))
input_labels = np.array([1]*len(fg_points) + [0]*len(bg_points))
# Bounding box (center region)
box = np.array([int(w*0.2),int(h*0.2), int(w*0.8), int(h*0.8)])
# =========================
# 5. PREDICT MASK
# =========================
masks, scores, logits = predictor.predict(
point_coords=input_points,
point_labels=input_labels,
box=box,
multimask_output=True
)
# =========================
# 6. SELECT BEST MASK
# =========================
best_mask = masks[np.argmax(scores)]
# =========================
# 7. POST-PROCESSING (IMPORTANT)
# =========================
mask = best_mask.astype(np.uint8) * 255
# Remove noise
kernel = np.ones((5,5), np.uint8)
mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)
mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)
# Keep largest connected component
num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(mask, connectivity=8)
largest_label = 1 + np.argmax(stats[1:, cv2.CC_STAT_AREA])
mask = np.where(labels == largest_label, 255, 0).astype(np.uint8)
# =========================
# 8. CREATE PROFESSIONAL OUTPUT (Alpha Blending + Refinement)
# =========================
# 1. Refine the mask edges to be smooth instead of jagged
# We use a Gaussian Blur to soften the transition and make it look anatomical
mask_binary = (mask == 255).astype(np.uint8)
kernel_size = 15
mask_smoothed = cv2.GaussianBlur(mask_binary.astype(float), (kernel_size, kernel_size), 0)
# 2. Create a Red Color Layer
# This layer is entirely red in the areas identified by the smoothed mask
red_layer = np.zeros_like(image_rgb)
red_layer[:] = [255, 0, 0] # Full Red layer
# 3. Apply the Alpha Blending
# result = (original_image) + (red_layer * mask_intensity * transparency)
# We use 0.3 as the alpha to ensure 70% of the tissue detail remains visible
alpha = 0.3
overlay = image_rgb.copy().astype(float)
for i in range(3): # Apply to R, G, and B channels
overlay[:, :, i] = image_rgb[:, :, i] * (1 - alpha * mask_smoothed) + \
red_layer[:, :, i] * (alpha * mask_smoothed)
# 4. Final Cleanup: Ensure the mask doesn't "bleed" into the black corners
# We only keep the overlay where the original image isn't pitch black
black_boundary = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2GRAY)
_, binary_boundary = cv2.threshold(black_boundary, 10, 255, cv2.THRESH_BINARY)
binary_boundary = binary_boundary / 255.0
for i in range(3):
overlay[:, :, i] = overlay[:, :, i] * binary_boundary
output_final = overlay.astype(np.uint8)
# =========================
# 9. SAVE OUTPUT
# =========================
# IMPORTANT: Convert back to BGR for OpenCV imwrite
cv2.imwrite(output_path, cv2.cvtColor(output_final, cv2.COLOR_RGB2BGR))
print(f"Professional high-fidelity segmentation saved to {output_path}")
