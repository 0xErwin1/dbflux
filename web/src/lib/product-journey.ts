export const JOURNEY_CHAPTERS = [
  { id: 'journey-explore', scene: 'explore' },
  { id: 'journey-builder', scene: 'builder' },
  { id: 'journey-edit', scene: 'edit' },
  { id: 'journey-visualize', scene: 'visualize' },
  { id: 'governance', scene: 'governance' },
] as const;

export type JourneyScene = (typeof JOURNEY_CHAPTERS)[number]['scene'];
export type JourneyChapterId = (typeof JOURNEY_CHAPTERS)[number]['id'];

export interface JourneyProgressState {
  progress: number;
  activeIndex: number;
  activeScene: JourneyScene;
  fromIndex: number;
  fromScene: JourneyScene;
  toIndex: number;
  toScene: JourneyScene;
  transition: number;
}

export interface JourneyChapterTarget {
  id: JourneyChapterId;
  index: number;
  scene: JourneyScene;
  progress: number;
}

export type JourneyEligibility =
  | { mode: 'enhanced'; reason: 'eligible' }
  | { mode: 'static'; reason: 'viewport' | 'reduced-motion' };

/** Maps native scroll progress to the nearest chapter and its active transition. */
export function journeyProgress(progress: number): JourneyProgressState {
  const normalized = Number.isNaN(progress) ? 0 : Math.min(1, Math.max(0, progress));
  const lastIndex = JOURNEY_CHAPTERS.length - 1;
  const position = normalized * lastIndex;
  const activeIndex = Math.round(position);
  const fromIndex = Math.floor(position);
  const toIndex = Math.min(fromIndex + 1, lastIndex);

  return {
    progress: normalized,
    activeIndex,
    activeScene: JOURNEY_CHAPTERS[activeIndex].scene,
    fromIndex,
    fromScene: JOURNEY_CHAPTERS[fromIndex].scene,
    toIndex,
    toScene: JOURNEY_CHAPTERS[toIndex].scene,
    transition: position - fromIndex,
  };
}

/** Resolves a fragment to the native-scroll position that shows that chapter. */
export function journeyChapterTarget(hash: string): JourneyChapterTarget | null {
  const id = hash.startsWith('#') ? hash.slice(1) : hash;
  const index = JOURNEY_CHAPTERS.findIndex((chapter) => chapter.id === id);
  if (index === -1) return null;

  return {
    ...JOURNEY_CHAPTERS[index],
    index,
    progress: index / (JOURNEY_CHAPTERS.length - 1),
  };
}

/** The static document remains the default until both enhancement conditions pass. */
export function journeyEligibility(
  viewportWidth: number,
  reducedMotion: boolean,
): JourneyEligibility {
  if (reducedMotion) return { mode: 'static', reason: 'reduced-motion' };
  if (viewportWidth <= 1180) return { mode: 'static', reason: 'viewport' };
  return { mode: 'enhanced', reason: 'eligible' };
}
